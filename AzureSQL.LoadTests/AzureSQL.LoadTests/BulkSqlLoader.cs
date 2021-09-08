using System.Data;
using System.Data.SqlClient;

namespace AzureSQL.LoadTests
{
    class BulkSqlLoader
    {
        private readonly string _connectionString;
        private readonly string _destinationTableName;

        public BulkSqlLoader(string connectionString, string destinationTableName)
        {
            _connectionString = connectionString;
            _destinationTableName = destinationTableName;
        }

        public void LoadData(IDataReader table)
        {
            using SqlConnection connection = new SqlConnection(_connectionString);
            connection.Open();
            using var bulkCopy =
                new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null)
                {
                    //BatchSize = table.Rows.Count,
                    BulkCopyTimeout = 0,
                    DestinationTableName = _destinationTableName
                };

            bulkCopy.WriteToServer(table);
        }

        public void LoadData(IDataReader dataReader, int batchSize)
        {
            using SqlConnection connection = new SqlConnection(_connectionString);
            connection.Open();
            using var bulkCopy =
                new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null)
                {
                    BulkCopyTimeout = 0,
                    BatchSize = batchSize,
                    DestinationTableName = _destinationTableName
                };

            bulkCopy.WriteToServer(dataReader);
        }
    }
}
