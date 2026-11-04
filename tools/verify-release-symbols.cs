using System.IO.Compression;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Text.Json;

if (args.Length != 3)
    throw new ArgumentException("Expected the library package, symbol package, and source commit.");

using var library = ZipFile.OpenRead(args[0]);
using var symbols = ZipFile.OpenRead(args[1]);
var embeddedSource = new Guid("0E8A571B-6926-466E-B4AD-8AB04611F5FE");
var sourceLink = new Guid("CC110556-A091-4D38-9FEC-25AB9A351A6A");
foreach (var framework in new[] { "net8.0", "net10.0" })
{
    using var dll = OpenEntry(library, $"lib/{framework}/Ksql.Linq.dll");
    using var pdbFile = OpenEntry(symbols, $"lib/{framework}/Ksql.Linq.pdb");
    using var pe = new PEReader(dll);
    var entry = pe.ReadDebugDirectory().Single(e => e.Type == DebugDirectoryEntryType.CodeView);
    var codeView = pe.ReadCodeViewDebugDirectoryData(entry);
    if (!IsNormalized(codeView.Path))
        throw new InvalidDataException($"Unexpected DLL debug path for {framework}.");
    if (!pe.TryOpenAssociatedPortablePdb(codeView.Path, _ => pdbFile, out var provider, out _) || provider is null)
        throw new InvalidDataException($"Portable PDB does not match the packaged {framework} DLL.");

    using (provider)
    {
        var reader = provider.GetMetadataReader();
        if (reader.Documents.Count == 0 ||
            reader.Documents.Any(handle => !IsNormalized(reader.GetString(reader.GetDocument(handle).Name))))
            throw new InvalidDataException($"Unexpected PDB document path for {framework}.");

        var debugInfo = reader.CustomDebugInformation.Select(reader.GetCustomDebugInformation).ToArray();
        if (debugInfo.Any(info => reader.GetGuid(info.Kind) == embeddedSource))
            throw new InvalidDataException($"Unexpected embedded source for {framework}.");

        var link = debugInfo.Single(info => reader.GetGuid(info.Kind) == sourceLink);
        using var json = JsonDocument.Parse(reader.GetBlobBytes(link.Value));
        var mapping = json.RootElement.GetProperty("documents").EnumerateObject().Single();
        if (mapping.Name != "/_/*" ||
            mapping.Value.GetString() != $"https://raw.githubusercontent.com/synthaicode/Ksql.Linq/{args[2]}/*")
            throw new InvalidDataException($"Unexpected Source Link mapping for {framework}.");
    }

    Console.WriteLine($"Verified {framework} Portable PDB, normalized paths and Source Link.");
}

static bool IsNormalized(string path) =>
    path.StartsWith("/_/", StringComparison.Ordinal) &&
    !path.Contains((char)92) && !path.Split('/').Contains("..");

static MemoryStream OpenEntry(ZipArchive archive, string path)
{
    using var input = (archive.GetEntry(path) ?? throw new InvalidDataException($"Missing {path}.")).Open();
    var output = new MemoryStream();
    input.CopyTo(output);
    output.Position = 0;
    return output;
}
