using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Exceptionless.Core.Extensions;
using Exceptionless.Core.Models;
using Exceptionless.Core.Repositories;
using Exceptionless.Core.Repositories.Configuration;
using Foundatio.Repositories.Elasticsearch.Extensions;
using Foundatio.Repositories.Migrations;
using Microsoft.Extensions.Logging;
using Nest;

namespace Exceptionless.Core.Migrations {
    public class EnsureStackStatus : MigrationBase {
        private readonly IElasticClient _client;
        private readonly ExceptionlessElasticConfiguration _config;

        public EnsureStackStatus(
            ExceptionlessElasticConfiguration configuration,
            ILoggerFactory loggerFactory
        ) : base(loggerFactory) {
            _config = configuration;
            _client = configuration.Client;
        }

        public override int? Version => 1;

        public override async Task RunAsync() {
            _logger.LogInformation("Start migration for adding stack status ...");
            _logger.LogInformation("Add status mappings to stack index.");
            var response = await _client.MapAsync<Stack>(d => d
                    .Index(_config.Stacks.VersionedName)
                    .Properties(p => p
                        .Keyword(f => f.Name(s => s.Status))
                        .Date(f => f.Name(s => s.SnoozeUntilUtc)))
            );
            _logger.LogTraceRequest(response);
            _logger.LogInformation("Done adding mapping for status.");
            
            _logger.LogInformation("Begin refreshing all indices");
            await _config.Client.Indices.RefreshAsync(Indices.All);
            _logger.LogInformation("Done refreshing all indices");
            
            var sw = Stopwatch.StartNew();

            _logger.LogInformation("Update Stack Status By Query");
            const string script = @"if (ctx._source.is_regressed == true) { ctx._source.status = 'regressed'; } else if (ctx._source.is_hidden == true) { ctx._source.status = 'ignored'; } else if (ctx._source.disable_notifications == true) { ctx._source.status = 'ignored'; } else if (ctx._source.is_fixed == true) { ctx._source.status = 'fixed'; } else { ctx._source.status = 'open'; }";
            var updateByQueryResponse = await _config.Client.UpdateByQueryAsync(new UpdateByQueryRequest(_config.Stacks.Name) {
                Script = new InlineScript(script),
                WaitForCompletion = false
            });
            
            if (updateByQueryResponse.IsValid) {
                long updated = await WaitForCompletionAsync(updateByQueryResponse, "Add stack status");
                _logger.LogInformation("Done calling update by query to add stack status: Updated {StacksUpdated:N0}", updated);
            } else {
                _logger.LogError("Error calling update by query to add stack status: {Message}", updateByQueryResponse.ApiCall.DebugInformation);
            }
            
            sw.Stop();
            _logger.LogInformation("Finished adding stack status: Time={Duration:d\\.hh\\:mm}", sw.Elapsed);
        }
        
        private async Task<long> WaitForCompletionAsync(UpdateByQueryResponse updateByQueryResponse, string description) {
            GetTaskResponse response;
            do {
                response = await _config.Client.Tasks.GetTaskAsync(updateByQueryResponse.Task, o => o.WaitForCompletion(false));
                if (response != null && !response.Completed) {
                    _logger.LogInformation("Task not completed yet for {Description}: RunningTimeInSeconds={RunningTime} Updated={Updated:N0} Total:{Total:N0} Task={TaskId}", description, (response.Task?.RunningTimeInNanoseconds ?? 0)/1000000000, response.Task?.Status?.Updated, response.Task?.Status?.Total, updateByQueryResponse.Task);
                    if (response.IsValid == false) {
                        _logger.LogWarning("Task response is invalid for {Description}: Message={DebugInformation} Task={TaskId}", description, response.ApiCall.DebugInformation, updateByQueryResponse.Task);
                        
                    }
                }
                
                await Task.Delay(5000).AnyContext();

            } while (response == null || !response.IsValid || !response.Completed);

            return response.Task?.Status?.Updated ?? 0;
        }   
    }
}