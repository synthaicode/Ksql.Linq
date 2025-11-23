using System.CommandLine;
using System.Diagnostics;
using System.Text;

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
            var guidePath = FindGuidePath();
            if (guidePath is null)
            {
                Console.Error.WriteLine("AI_ASSISTANT_GUIDE.md not found next to the CLI assembly or in the working directory.");
                Environment.ExitCode = 1;
                return;
            }

            var header = """
            # How to use this text with your AI assistant

            Copy this entire output and paste it into your AI assistant, or save it as a file and ask:

            "Read this Ksql.Linq AI Assistant Guide and act as a design support AI for my Ksql.Linq project."

            ---

            """;

            var footer = """

            ---
            Note: This guide is specific to your installed Ksql.Linq version. When you upgrade Ksql.Linq, rerun `dotnet ksql ai-assist` and share the new output with your AI assistant.
            """;

            var guide = await File.ReadAllTextAsync(guidePath, Encoding.UTF8);
            var fullText = header + guide + footer;

            Console.OutputEncoding = Encoding.UTF8;
            Console.Write(fullText);

            if (copy)
            {
                TryCopyToClipboard(fullText);
            }
        }, copyOption);

        return command;
    }

    private static string? FindGuidePath()
    {
        var assemblyDir = AppContext.BaseDirectory;
        var candidate = Path.Combine(assemblyDir, "AI_ASSISTANT_GUIDE.md");
        if (File.Exists(candidate))
        {
            return candidate;
        }

        candidate = Path.Combine(Directory.GetCurrentDirectory(), "AI_ASSISTANT_GUIDE.md");
        return File.Exists(candidate) ? candidate : null;
    }

    private static void TryCopyToClipboard(string text)
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                var psi = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = "/c clip",
                    RedirectStandardInput = true,
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
}
