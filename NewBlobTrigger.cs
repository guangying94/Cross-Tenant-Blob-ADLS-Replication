using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using Azure.Identity;
using System.Threading.Tasks;

namespace CrossTenantBlobADLSReplication
{
    public class NewBlobTrigger
    {
        [FunctionName("NewBlobTrigger")]
        public async Task Run(
            [BlobTrigger("source/{name}",
            Connection = "SOURCE_BLOB_CONNECTION_STRING")]Stream myBlob, 
            string name, 
            ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes.");

            try
            {
                // Check if parameters have ADLS account key
                // If exist --> use Account key to authenticate to ADLS, else it will use MI to authenticate
                string containerName = Environment.GetEnvironmentVariable("DEST_CONTAINER");
                
                if (Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY") == null)
                {
                    DataLakeServiceClient adlsServiceClient = GetDataLakeServiceClientWithMI();
                    DataLakeFileSystemClient fileSystemClient = adlsServiceClient.GetFileSystemClient(containerName);
                    await UploadFile(fileSystemClient, name, myBlob);
                }

                else
                {
                    DataLakeServiceClient adlsServiceClient = GetDataLakeServiceClientWithAccountKey();
                    DataLakeFileSystemClient fileSystemClient = adlsServiceClient.GetFileSystemClient(containerName);
                    await UploadFile(fileSystemClient, name, myBlob);
                }

                log.LogInformation($"{name} is uplaoded successfully.");
            }
            catch(Exception ex)
            {
                log.LogError(ex.Message );
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
        public static DataLakeServiceClient GetDataLakeServiceClientWithAccountKey()
        {
            string accountName = Environment.GetEnvironmentVariable("DEST_ACCOUNT_NAME");
            string accountKey = Environment.GetEnvironmentVariable("DEST_ACCOUNT_KEY");

            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);

            return dataLakeServiceClient;
        }

        // Upload to directory
        public async Task UploadFile(DataLakeFileSystemClient fileSystemClient, string fileName, Stream fileContent)
        {
            string destDirectory = Environment.GetEnvironmentVariable("DEST_DIRECTORY");

            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(destDirectory);

            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(fileName);

            long fileSize = fileContent.Length;

            await fileClient.AppendAsync(fileContent, offset: 0);
            await fileClient.FlushAsync(position: fileSize);
        }
    }
}
