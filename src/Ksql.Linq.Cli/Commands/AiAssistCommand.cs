using System.CommandLine;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Reflection;

namespace Ksql.Linq.Cli.Commands;

public static class AiAssistCommand
{
    public static Command Create()
    {
        var command = new Command("ai-assist", "Prints the Ksql.Linq AI Assistant Guide for use with AI coding assistants.")
        {
            new Option<bool>(
                name: "--copy",
                description: "Copy the guide text to the clipboard (where supported).")
        };

        var copyOption = command.Options.OfType<Option<bool>>().Single();

        command.SetHandler(async (bool copy) =>
        {
            var guideText = await ReadGuideTextAsync();
            if (guideText is null)
            {
                Console.Error.WriteLine("AI_ASSISTANT_GUIDE.md not found (library-embedded or local file).");
                Environment.ExitCode = 1;
                return;
            }

            var culture = CultureInfo.CurrentUICulture.TwoLetterISOLanguageName.ToLowerInvariant();

            var header = await GetHeaderAsync(culture);
            var footer = await GetFooterAsync(culture);

            var fullText = header + guideText + footer;

            Console.OutputEncoding = Encoding.UTF8;
            Console.Write(fullText);

            if (copy)
            {
                TryCopyToClipboard(fullText);
            }
        }, copyOption);

        return command;
    }

    private static async Task<string?> ReadGuideTextAsync()
    {
        // Preferred: read the guide embedded in the Ksql.Linq library assembly (bundled with the library package).
        var embedded = TryReadEmbeddedGuide();
        if (!string.IsNullOrWhiteSpace(embedded))
            return embedded;

        // Fallback: local file next to the CLI tool (legacy) or in the working directory.
        var assemblyDir = AppContext.BaseDirectory;
        var candidate = Path.Combine(assemblyDir, "AI_ASSISTANT_GUIDE.md");
        if (File.Exists(candidate))
            return await File.ReadAllTextAsync(candidate, Encoding.UTF8);

        candidate = Path.Combine(Directory.GetCurrentDirectory(), "AI_ASSISTANT_GUIDE.md");
        return File.Exists(candidate) ? await File.ReadAllTextAsync(candidate, Encoding.UTF8) : null;
    }

    private static string? TryReadEmbeddedGuide()
    {
        try
        {
            var asm = typeof(Ksql.Linq.KsqlContext).Assembly;
            // Prefer the known logical name; fall back to suffix search for safety.
            var resourceName = asm.GetManifestResourceNames()
                .FirstOrDefault(n => string.Equals(n, "Ksql.Linq.AI_ASSISTANT_GUIDE.md", StringComparison.Ordinal))
                ?? asm.GetManifestResourceNames().FirstOrDefault(n => n.EndsWith("AI_ASSISTANT_GUIDE.md", StringComparison.OrdinalIgnoreCase));
            if (string.IsNullOrWhiteSpace(resourceName))
                return null;

            using var s = asm.GetManifestResourceStream(resourceName);
            if (s is null) return null;
            using var r = new StreamReader(s, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            return r.ReadToEnd();
        }
        catch
        {
            return null;
        }
    }

    private static void TryCopyToClipboard(string text)
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                // Prefer PowerShell's Set-Clipboard to avoid codepage-dependent mojibake (multi-locale safe).
                // Read stdin explicitly as UTF-8 inside PowerShell, so the parent can always write UTF-8 bytes.
                var psScript =
                    "$sr = New-Object System.IO.StreamReader([Console]::OpenStandardInput(), [System.Text.UTF8Encoding]::new($false));" +
                    " $t = $sr.ReadToEnd();" +
                    " Set-Clipboard -Value $t";

                if (!RunPipeCommand("pwsh", text, "-NoProfile", "-NonInteractive", "-Command", psScript))
                {
                    if (!RunPipeCommand("powershell", text, "-NoProfile", "-NonInteractive", "-Command", psScript))
                    {
                        // Fallback: clip.exe (best-effort; may be codepage-dependent on some systems).
                        var psi = new ProcessStartInfo
                        {
                            FileName = "cmd.exe",
                            Arguments = "/c clip",
                            RedirectStandardInput = true,
                            StandardInputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
                            UseShellExecute = false,
                            CreateNoWindow = true
                        };
                        using var p = Process.Start(psi);
                        if (p is not null)
                        {
                            p.StandardInput.Write(text);
                            p.StandardInput.Close();
                            p.WaitForExit();
                        }
                    }
                }
            }
            else if (OperatingSystem.IsMacOS())
            {
                RunPipeCommand("pbcopy", text);
            }
            else if (OperatingSystem.IsLinux())
            {
                // Try wl-copy, then xclip
                if (!RunPipeCommand("wl-copy", text))
                {
                    RunPipeCommand("xclip", text, "-selection", "clipboard");
                }
            }
        }
        catch
        {
            // Clipboard copy is best-effort; ignore failures.
        }
    }

    private static bool RunPipeCommand(string fileName, string input, params string[] args)
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = fileName,
                RedirectStandardInput = true,
                StandardInputEncoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
                UseShellExecute = false,
                CreateNoWindow = true
            };
            foreach (var a in args)
            {
                psi.ArgumentList.Add(a);
            }

            using var p = Process.Start(psi);
            if (p is null) return false;

            p.StandardInput.Write(input);
            p.StandardInput.Close();
            p.WaitForExit();

            return p.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    private const string EnglishHeader = """
    # How to use this text with your AI assistant

    Copy this entire output and paste it into your AI assistant, or save it as a file and ask:

    "Read this Ksql.Linq AI Assistant Guide and act as a design support AI for my Ksql.Linq project."

    ---

    """;

    private const string EnglishFooter = """

    ---
    Note: This guide is specific to your installed Ksql.Linq version. When you upgrade Ksql.Linq, rerun `dotnet ksql ai-assist` and share the new output with your AI assistant.
    """;

    private static async Task<string> GetHeaderAsync(string culture)
    {
        if (culture == "en")
        {
            return EnglishHeader;
        }

        var localized = await TryFetchLocaleAsync("ai_assist_header", culture);
        return localized ?? EnglishHeader;
    }

    private static async Task<string> GetFooterAsync(string culture)
    {
        if (culture == "en")
        {
            return EnglishFooter;
        }

        var localized = await TryFetchLocaleAsync("ai_assist_footer", culture);
        return localized ?? EnglishFooter;
    }

    private static async Task<string?> TryFetchLocaleAsync(string baseName, string culture)
    {
        try
        {
            using var client = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(1)
            };

            var url =
                $"https://raw.githubusercontent.com/synthaicode/Ksql.Linq/main/docs/ai_support_cli/messages/{baseName}_{culture}.txt";

            var content = await client.GetStringAsync(url).ConfigureAwait(false);
            return string.IsNullOrWhiteSpace(content) ? null : content;
        }
        catch
        {
            return null;
        }
    }
}
