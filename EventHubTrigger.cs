using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Storage.Files.DataLake;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CrossTenantBlobADLSReplication
{
    public static class EventHubTrigger
    {
        [FunctionName("EventHubTrigger")]
        public static async Task Run([EventHubTrigger("%TRIGGER_EVENT_HUB_NAME%", Connection = "TRIGGER_EVENT_HUB_CONNECTION")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                string incomingRawEvent = eventData.EventBody.ToString();
                log.LogInformation($"C# Event Hub trigger function processed a message: {incomingRawEvent}");
                await Task.Yield();

                IncomingEventHubMessageBody[] incomingEvent = JsonConvert.DeserializeObject<IncomingEventHubMessageBody[]>(incomingRawEvent);
                string uploadedFileUrl = incomingEvent.FirstOrDefault().data.blobUrl;
                Uri fileUri = new Uri(uploadedFileUrl);

                string fileName = Path.GetFileName(uploadedFileUrl);
                string[] segments = fileUri.Segments;
                string filePath = System.Web.HttpUtility.UrlDecode(string.Join("", segments[2..]));

                // Get Environment Variables
                bool loggingWithEventHub = Convert.ToBoolean(Environment.GetEnvironmentVariable("USE_EVENT_HUB"));
                string sourceContainerName = Environment.GetEnvironmentVariable("SOURCE_CONTAINER");
                string destContainerName = Environment.GetEnvironmentVariable("DEST_CONTAINER");

                try
                {
                    // Initialize source ADLS client
                    DataLakeServiceClient sourceAdlsClient = HelperMethods.GetDataLakeServiceClient(
                        Environment.GetEnvironmentVariable("SOURCE_ACCOUNT_NAME"),
                        Environment.GetEnvironmentVariable("SOURCE_ACCOUNT_KEY"));
                    DataLakeFileSystemClient sourceFileSystemClient = sourceAdlsClient.GetFileSystemClient(sourceContainerName);

                    //Read file from source ADLS
                    Stream fileContent = HelperMethods.ReadFileFromADLS(sourceFileSystemClient, filePath);

                    // Initialize destination ADLS client
                    DataLakeServiceClient destAdlsClient = HelperMethods.GetDataLakeServiceClient(
                        Environment.GetEnvironmentVariable("DEST_ACCOUNT_NAME"),
                        Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY"));

                    DataLakeFileSystemClient destFileSystemClient = destAdlsClient.GetFileSystemClient(destContainerName);
                    await HelperMethods.UploadFileToADLS(destFileSystemClient, fileName, fileContent, log);

                    if (loggingWithEventHub)
                    {
                        EventHubMessageBody messageBody = new EventHubMessageBody();
                        messageBody.FileUrl = uploadedFileUrl;
                        messageBody.Status = "Success";
                        await HelperMethods.SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                    }
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                    log.LogError(e.Message);
                    if (loggingWithEventHub)
                    {
                        EventHubMessageBody messageBody = new EventHubMessageBody();
                        messageBody.FileUrl = uploadedFileUrl;
                        messageBody.Status = "Failed";
                        await HelperMethods.SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                    }
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
