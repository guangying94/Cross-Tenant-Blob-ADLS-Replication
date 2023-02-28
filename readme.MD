# Sample code for Azure Functions - cross tenant blob to ADLS file replication
This repository contains sample code for Azure Functions to replicate file between 2 Azure tenants.

## Details
This function monitor files created in **source** folder in blob storage. Update the trigger accordingly.

```csharp
        public async Task Run(
            [BlobTrigger("source/{name}",  //update the monitored resource accordingly
            Connection = "SOURCE_BLOB_CONNECTION_STRING")]Stream myBlob, 
            string name, 
            ILogger log)
```

For development locally, rename the file _sample.local.settings.json_ to _local.settings.json_.

### Authentication
The data source in this case, is Azure Blob Storage, and the destination is Azure Data Lake Store Gen 2.

This Azure Functions runs in destination tenant, hence not able to use managed identity to authenticate to blob storage in source tenant. Connection string is used instead. Update the _local.settings.json_ accordingly, or application settings once deployed. For security purposes, Azure Key Vault can be used to protect connection string of blob storage.

On the other hand, since Azure Functions and ADLS Gen 2 are running in the same tenant, managed identity can be used for authentication. Enable managed identity in Azure Functions and update ADLS access control list (ACL) accordingly.

### Architecture
![architecture.png](assets/architecture.png)