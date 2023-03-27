using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace CrossTenantBlobADLSReplication
{
    public class VarBlobTrigger
    {
        [FunctionName("VarBlobTrigger")]
        public async Task Run([BlobTrigger("%SOURCE_BLOB_CONTAINER%/{name}", Connection = "SOURCE_BLOB_CONNECTION_STRING")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes.");
            bool loggingWithEventHub = Convert.ToBoolean(Environment.GetEnvironmentVariable("USE_EVENT_HUB"));

            try
            {
                string containerName = Environment.GetEnvironmentVariable("DEST_CONTAINER");

                DataLakeServiceClient destAdlsServiceClient = HelperMethods.GetDataLakeServiceClient(
                    Environment.GetEnvironmentVariable("DEST_ACCOUNT_NAME"),
                    Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY"));

                DataLakeFileSystemClient destAdlsFileSystemClient = destAdlsServiceClient.GetFileSystemClient(containerName);
                await HelperMethods.UploadFileToADLS(destAdlsFileSystemClient, name, myBlob, log);

                if (loggingWithEventHub)
                {
                    EventHubMessageBody messageBody = new EventHubMessageBody();
                    messageBody.FileUrl = name;
                    messageBody.Status = "Success";
                    await HelperMethods.SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                }

                log.LogInformation($"{name} is uplaoded successfully.");
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                if (loggingWithEventHub)
                {
                    EventHubMessageBody messageBody = new EventHubMessageBody();
                    messageBody.FileUrl = name;
                    messageBody.Status = "Failed";
                    await HelperMethods.SendMessageToEventHub(JsonConvert.SerializeObject(messageBody), log);
                }
            }
        }
    }
}
