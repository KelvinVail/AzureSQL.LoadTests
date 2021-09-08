using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace AzureSQL.LoadTests
{
    public class BlobTests
    {
        public BlobClient GetBlob(string conn)
        {
            BlobContainerClient container = new BlobContainerClient(conn, "nyctaxi");
            container.CreateIfNotExists(PublicAccessType.Blob);

            return container.GetBlobClient("yellow_tripdata_2018-01.csv");
        }
    }
}
