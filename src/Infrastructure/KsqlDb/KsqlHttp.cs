using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Ksql.Linq.Infrastructure.KsqlDb;

/// <summary>
/// Thin helper over HttpClient for ksqlDB endpoints.
/// Centralizes JSON content creation and streaming send patterns.
/// </summary>
internal sealed class KsqlHttp
{
    private readonly HttpClient _client;

    public KsqlHttp(HttpClient client)
    {
        _client = client;
    }

    public async Task<HttpResponseMessage> PostJsonAsync(string path, object payload, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        return await _client.PostAsync(path, content, cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> PostJsonReadBodyAsync(string path, object payload, CancellationToken cancellationToken = default)
    {
        using var response = await PostJsonAsync(path, payload, cancellationToken).ConfigureAwait(false);
        return await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<HttpResponseMessage> SendJsonStreamAsync(string path, object payload, HttpCompletionOption completionOption, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(payload);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");
        var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = content };
        return await _client.SendAsync(request, completionOption, cancellationToken).ConfigureAwait(false);
    }
}

