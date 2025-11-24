using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace PhysicalTestEnv.Logging;

/// <summary>
/// Provides a shared logger factory for physical tests that writes to both console and a log file.
/// </summary>
public static class PhysicalTestLog
{
    private static readonly object Sync = new();
    private static FileLoggerProvider? _fileProvider;
    private static string _logPath = string.Empty;

    public static ILoggerFactory CreateFactory(
        string suiteName,
        LogLevel minimumLevel,
        Action<ILoggingBuilder>? configure = null)
    {
        if (string.IsNullOrWhiteSpace(suiteName))
            suiteName = "physicalTests";

        var path = EnsureLogPath();

        lock (Sync)
        {
            _fileProvider ??= new FileLoggerProvider(path);
            _fileProvider.RegisterSuite(suiteName);
        }

        return LoggerFactory.Create(builder =>
        {
            builder.ClearProviders();
            builder.SetMinimumLevel(minimumLevel);
            builder.AddConsole();
            builder.AddProvider(_fileProvider!);
            // Default: capture Streamiz logs, but suppress noisy Processor-level debug output
            builder.AddFilter("Streamiz.Kafka.Net.Processors", LogLevel.Information);
            builder.AddFilter("Streamiz.Kafka.Net", LogLevel.Debug);
            builder.AddFilter("Streamiz", LogLevel.Debug);
            configure?.Invoke(builder);
        });
    }

    private static string EnsureLogPath()
    {
        if (!string.IsNullOrEmpty(_logPath))
            return _logPath;

        // reports/physical/physical_tests.log relative to repo root (bin/Debug/... during tests)
        var baseDir = AppContext.BaseDirectory;
        var candidate = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "reports", "physical"));
        Directory.CreateDirectory(candidate);
        _logPath = Path.Combine(candidate, "physical_tests.log");
        return _logPath;
    }

    private sealed class FileLoggerProvider : ILoggerProvider
    {
        private readonly string _path;
        private readonly object _writeLock = new();
        private string _suiteLabel = string.Empty;

        public FileLoggerProvider(string path)
        {
            _path = path;
        }

        public ILogger CreateLogger(string categoryName)
            => new FileLogger(_path, categoryName, () => _suiteLabel, _writeLock);

        public void Dispose()
        {
            // nothing to dispose
        }

        public void RegisterSuite(string suiteName)
        {
            _suiteLabel = suiteName ?? string.Empty;
            lock (_writeLock)
            {
                File.AppendAllText(_path, $"{Environment.NewLine}=== {DateTimeOffset.UtcNow:O} :: {suiteName} ==={Environment.NewLine}");
            }
        }
    }

    private sealed class FileLogger : ILogger
    {
        private readonly string _path;
        private readonly string _category;
        private readonly Func<string> _suite;
        private readonly object _writeLock;

        public FileLogger(string path, string category, Func<string> suite, object writeLock)
        {
            _path = path;
            _category = category;
            _suite = suite;
            _writeLock = writeLock;
        }

        public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (formatter == null) return;

            var message = formatter(state, exception);
            var line = $"{DateTimeOffset.UtcNow:O} [{_suite()}] {logLevel,-11} {_category} :: {message}";
            if (exception != null)
            {
                line += Environment.NewLine + exception;
            }

            lock (_writeLock)
            {
                File.AppendAllText(_path, line + Environment.NewLine);
            }
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
