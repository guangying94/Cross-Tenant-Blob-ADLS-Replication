// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventGrid;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using Azure.Identity;
using System.IO;
using System.Threading.Tasks;
using System.Reflection.Metadata;
using Newtonsoft.Json;
using Microsoft.Extensions.ObjectPool;
using Azure.Storage.Files.DataLake.Models;
using Azure;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using System.Text;

namespace CrossTenantBlobADLSReplication
{
    public static class EventGridADLS
    {
        [FunctionName("EventGridFileTrigger")]
        public static async Task Run([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            string eventContent = eventGridEvent.Data.ToString();
            EventGridBody eventBody = JsonConvert.DeserializeObject<EventGridBody>(eventContent);
            string url = eventBody.blobUrl;
            Uri fileUri = new Uri(url);

            string fileName = Path.GetFileName(url);
            string[] segments = fileUri.Segments;
            string filePath = System.Web.HttpUtility.UrlDecode(string.Join("", segments[2..]));

            // Get Environment Variables
            bool loggingWithEventHub = Convert.ToBoolean(Environment.GetEnvironmentVariable("USE_EVENT_HUB"));
            string sourceContainerName = Environment.GetEnvironmentVariable("SOURCE_CONTAINER");
            string destContainerName = Environment.GetEnvironmentVariable("DEST_CONTAINER");

            try
            {
                // Initialize source ADLS client
                DataLakeServiceClient sourceAdlsClient = GetDataLakeServiceClientWithAccountKey(
                    Environment.GetEnvironmentVariable("SOURCE_ACCOUNT_NAME"),
                    Environment.GetEnvironmentVariable("SOURCE_ACCOUNT_KEY"));
                DataLakeFileSystemClient sourceFileSystemClient = sourceAdlsClient.GetFileSystemClient(sourceContainerName);

                //Read file from source ADLS
                Stream fileContent = ReadFile(sourceFileSystemClient, filePath);

                // Use MI to authenticate to destination ADLS
                if (Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY") == null)
                {
                    //Initialize dest ADLS client
                    DataLakeServiceClient destAdlsServiceClient = GetDataLakeServiceClientWithMI();
                    DataLakeFileSystemClient fileSystemClient = destAdlsServiceClient.GetFileSystemClient(destContainerName);
                    await UploadFile(fileSystemClient, fileName, fileContent);
                    if(loggingWithEventHub)
                    {
                        EventHubMessageBody messageBody = new EventHubMessageBody();
                        messageBody.FileUrl = url;
                        messageBody.Status = "Success";
                        await SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                    }
                }
                // Use account key to authenticate to destination ADLS
                else
                {
                    //Initialize dest ADLS client
                    DataLakeServiceClient adlsServiceClient = GetDataLakeServiceClientWithAccountKey(
                        Environment.GetEnvironmentVariable("DEST_ACCOUNT_NAME"),
                        Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY"));
                    DataLakeFileSystemClient fileSystemClient = adlsServiceClient.GetFileSystemClient(destContainerName);
                    await UploadFile(fileSystemClient, fileName, fileContent);
                    if (loggingWithEventHub)
                    {
                        EventHubMessageBody messageBody = new EventHubMessageBody();
                        messageBody.FileUrl = url;
                        messageBody.Status = "Success";
                        await SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                    }
                }
            }
            catch (Exception ex)
            {
                if (loggingWithEventHub)
                {
                    EventHubMessageBody messageBody = new EventHubMessageBody();
                    messageBody.FileUrl = url;
                    messageBody.Status = "Failed";
                    await SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                }
                log.LogError(ex.Message);
            }
        }

        // Use AAD to authenticate - enable MI in Azure Functions
        public static DataLakeServiceClient GetDataLakeServiceClientWithMI()
        {
            string accountName = Environment.GetEnvironmentVariable("DEST_ACCOUNT_NAME");
            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri),
                                    new DefaultAzureCredential());

            return dataLakeServiceClient;
        }

        // Use Account key to authenticate
        public static DataLakeServiceClient GetDataLakeServiceClientWithAccountKey(string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);

            return dataLakeServiceClient;
        }

        //Read file from data lake
        public static Stream ReadFile(DataLakeFileSystemClient fileSystemClient, string filePath)
        {
            DataLakeFileClient fileClient = fileSystemClient.GetFileClient(filePath);
            return fileClient.OpenRead();
        }

        // Upload to directory
        public static async Task UploadFile(DataLakeFileSystemClient fileSystemClient, string fileName, Stream fileContent)
        {
            string destDirectory = Environment.GetEnvironmentVariable("DEST_DIRECTORY");

            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(destDirectory);

            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(fileName);

            long fileSize = fileContent.Length;

            await fileClient.AppendAsync(fileContent, offset: 0);
            await fileClient.FlushAsync(position: fileSize);
        }

        public static async Task SendMessageToEventHub(string message, ILogger log)
        {
            string eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME");
            string eventHubConnectionString = Environment.GetEnvironmentVariable("EVENT_HUB_CONNECTION_STRING");
            
            //If event hub connection string exist, then use password to authenticate
            // else use Azure MI to authenticate
            if(eventHubConnectionString != null)
            {
                EventHubProducerClient producerClient = new EventHubProducerClient(eventHubConnectionString,eventHubName);
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));
                try
                {
                    await producerClient.SendAsync(eventBatch);
                    log.LogInformation($"Event Hub message content: {message}");
                }
                catch(Exception ex)
                {
                    log.LogError(ex.Message);
                }
            }
            else
            {
                string eventHubNamespaceName = Environment.GetEnvironmentVariable("EVENT_HUB_NAMESPACE_NAME");

                EventHubProducerClient producerClient = new EventHubProducerClient($"{eventHubNamespaceName}.servicebus.windows.net",eventHubName, new DefaultAzureCredential());
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));
                try
                {
                    await producerClient.SendAsync(eventBatch);
                    log.LogInformation($"Event Hub message content: {message}");
                }
                catch (Exception ex)
                {
                    log.LogError(ex.Message);
                }
            }
        }
    }
}
