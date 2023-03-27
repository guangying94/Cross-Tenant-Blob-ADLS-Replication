using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Azure.Messaging.EventHubs.Producer;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging;

namespace CrossTenantBlobADLSReplication
{
    public class HelperMethods
    {
        // Get ADLS Service Client. If account ket exists, use account key to authenticate, else it uses Maanaged identity to authenticate
        public static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string? accountKey)
        {
            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            if (accountKey == null)
            {
                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri),
                                        new DefaultAzureCredential());

                return dataLakeServiceClient;
            }
            else
            {
                StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient
                    (new Uri(dfsUri), sharedKeyCredential);

                return dataLakeServiceClient;
            }
        }

        //Read file from data lake
        public static Stream ReadFileFromADLS(DataLakeFileSystemClient fileSystemClient, string filePath)
        {
            DataLakeFileClient fileClient = fileSystemClient.GetFileClient(filePath);
            return fileClient.OpenRead();
        }

        // Upload to directory
        public static async Task UploadFileToADLS(DataLakeFileSystemClient fileSystemClient, string fileName, Stream fileContent, ILogger log)
        {
            string destDirectory = Environment.GetEnvironmentVariable("DEST_DIRECTORY");

            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(destDirectory);

            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(fileName);

            long fileSize = fileContent.Length;

            await fileClient.AppendAsync(fileContent, offset: 0);
            await fileClient.FlushAsync(position: fileSize);
            log.LogInformation($"{fileName} is uploaded");
        }

        public static async Task SendMessageToEventHub(string message, ILogger log)
        {
            string eventHubName = Environment.GetEnvironmentVariable("EVENT_HUB_NAME");
            string eventHubConnectionString = Environment.GetEnvironmentVariable("EVENT_HUB_CONNECTION_STRING");

            // If event hub connection string exist, then use password to authenticate
            // else use Azure MI to authenticate
            if (eventHubConnectionString != null)
            {
                EventHubProducerClient producerClient = new EventHubProducerClient(eventHubConnectionString, eventHubName);
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
            else
            {
                string eventHubNamespaceName = Environment.GetEnvironmentVariable("EVENT_HUB_NAMESPACE_NAME");

                EventHubProducerClient producerClient = new EventHubProducerClient($"{eventHubNamespaceName}.servicebus.windows.net", eventHubName, new DefaultAzureCredential());
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
