using IdentityModel.Client;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace OmniaMigrationTool
{
    public class ApiClient
    {
        private readonly Uri _apiEndpoint;
        private readonly string _clientId;
        private readonly string _clientSecret;
        private readonly Uri _identityEndpoint;
        private readonly MemoryCache _cache;

        public ApiClient(Uri apiEndpoint, Uri identityEndpoint, string clientId, string clientSecret, MemoryCache cache)
        {
            _apiEndpoint = apiEndpoint;
            _identityEndpoint = identityEndpoint;
            _clientId = clientId;
            _clientSecret = clientSecret;
            _cache = cache;
        }

        public async Task PatchAsync(string tenant, string environment, string area, string definition, string code, string requestBody)
        {
            var accessToken = await GetJwtTokenAsync();

            var authValue = new AuthenticationHeaderValue("Bearer", accessToken);
            var client = new HttpClient { DefaultRequestHeaders = { Authorization = authValue } };

            var apiEndpoint = $"{_apiEndpoint}{tenant}/{environment}/{area}/{definition}/{code}";

            var requestContent = new StringContent(requestBody, Encoding.UTF8, "application/json");
            var requestResult = await client.PatchAsync(apiEndpoint, requestContent);

            if (!requestResult.IsSuccessStatusCode)
            {
                if (requestResult.StatusCode == System.Net.HttpStatusCode.Forbidden || requestResult.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                {
                    throw new InvalidOperationException($"Error when updating the record '{code}': This application doesn't have permissions to do the request");
                }
                else
                {
                    var responseBody = requestResult.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    throw new InvalidOperationException($"Error when updating the record '{code}': {responseBody} ({requestResult.StatusCode.ToString()}).");
                }
            }
        }


        private async Task<string> GetJwtTokenAsync(bool ignoringCache = false)
        {
            if (!ignoringCache && _cache.TryGetValue("access_token", out string accessToken))
            {
                return accessToken;
            }
            
            // discover endpoints from metadata
            var discoveryClient = new DiscoveryClient(_identityEndpoint.ToString()) { Policy = { RequireHttps = false } };

            var disco = await discoveryClient.GetAsync();
            if (disco.IsError)
            {
                throw new InvalidDataException($"Error invoking the Discovery Endpoint: {disco.Error}");
            }

            //request token
            var tokenClient = new TokenClient(disco.TokenEndpoint, _clientId, _clientSecret);
            var tokenResponse = await tokenClient.RequestClientCredentialsAsync("api");

            if (!tokenResponse.IsError)
            {
                var cacheEntryOptions = new MemoryCacheEntryOptions()
                    .SetAbsoluteExpiration(DateTimeOffset.UtcNow.AddSeconds(tokenResponse.ExpiresIn - 10));

                _cache.Set("access_token", tokenResponse.AccessToken, cacheEntryOptions);
                return tokenResponse.AccessToken;
            }

            throw new InvalidDataException($"Error requesting a new Token: {tokenResponse.Error}");

        }
    }
}