using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace AzureSQL.LoadTests.Function
{
    public static class StreamFile
    {
        [Function("StreamFile")]
        public static HttpResponseData Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            var logger = executionContext.GetLogger("StreamFile");
            logger.LogInformation("C# HTTP trigger function processed a request.");

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "text/plain; charset=utf-8");

            response.WriteString("Welcome to Azure Functions!");

            var timer = new Stopwatch();
            timer.Reset();
            timer.Start();
            ReadFileTests.ToSqlServer(
                Environment.GetEnvironmentVariable("SQL_CONN"),
                Environment.GetEnvironmentVariable("BLOB_CONN"));
            timer.Stop();

            response.WriteString("Streamed in: " + timer.ElapsedMilliseconds);
            return response;
        }
    }
}
