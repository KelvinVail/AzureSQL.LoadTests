using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using CsvHelper;
using Parquet;
using Parquet.Data;
using Sylvan.Data;
using Sylvan.Data.Csv;
using CsvDataReader = Sylvan.Data.Csv.CsvDataReader;
using DataColumn = Parquet.Data.DataColumn;

namespace AzureSQL.LoadTests
{
    public static class ReadFileTests
    {
        public static int StreamReaderCount(string filePath)
        {
            using StreamReader r = new (filePath);
            int i = 0;
            while (r.ReadLine() != null) 
                i++; 

            return i;
        }

        public static int CsvHelper(string filePath)
        {
            using StreamReader reader = new(filePath);
            var config =
                new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture) { CacheFields = true, };
            var csvParser = new CsvParser(reader, config);
            int i = 0;
            while (csvParser.Read())
            {
                var record = csvParser.Record;
                i++;
            }

            return i;
        }

        public static int Sylvan(string filePath)
        {
            using StreamReader reader = new (filePath);
            var options = new CsvDataReaderOptions
            {
                HasHeaders = true,
                BufferSize = 0x10000,
            };

            using var csvReader = CsvDataReader.Create(reader, options);
            int i = 0;
            csvReader.Read();
            while (csvReader.Read())
            {
                var record = new Trip();
                record.VendorID = csvReader.GetInt32(0);
                record.tpep_pickup_datetime = csvReader.GetDateTime(1);
                record.tpep_dropoff_datetime = csvReader.GetDateTime(2);
                record.passenger_count = csvReader.GetInt32(3);
                record.trip_distance = csvReader.GetDecimal(4);
                record.RatecodeID = csvReader.GetInt32(5);
                record.store_and_fwd_flag = csvReader.GetChar(6);
                record.PULocationID = csvReader.GetInt32(7);
                record.DOLocationID = csvReader.GetInt32(8);
                record.payment_type = csvReader.GetInt32(9);
                record.fare_amount = csvReader.GetDecimal(10);
                record.extra = csvReader.GetDecimal(11);
                record.mta_tax = csvReader.GetDecimal(12);
                record.tip_amount = csvReader.GetDecimal(13);
                record.tolls_amount = csvReader.GetDecimal(14);
                record.improvement_surcharge = csvReader.GetDecimal(15);
                record.total_amount = csvReader.GetDecimal(16);
                i++;
            }

            return i;
        }

        public static int ToParquet(string filePath, string destination)
        {
            using StreamReader reader = new (filePath);
            var options = new CsvDataReaderOptions
            {
                HasHeaders = true,
                BufferSize = 0x10000,
            };

            var trips = new Trips();

            using var csvReader = CsvDataReader.Create(reader, options);
            int i = 0;
            csvReader.Read();
            while (csvReader.Read())
            {
                trips.VendorID.Add(csvReader.GetInt32(0));
                //trips.tpep_pickup_datetime.Add(csvReader.GetDateTime(1));
                //trips.tpep_dropoff_datetime.Add(csvReader.GetDateTime(2));
                //trips.passenger_count.Add(csvReader.GetInt32(3));
                //trips.trip_distance.Add(csvReader.GetDecimal(4));
                //trips.RatecodeID.Add(csvReader.GetInt32(5));
                //trips.store_and_fwd_flag.Add(csvReader.GetChar(6));
                //trips.PULocationID.Add(csvReader.GetInt32(7));
                //trips.DOLocationID.Add(csvReader.GetInt32(8));
                //trips.payment_type.Add(csvReader.GetInt32(9));
                //trips.fare_amount.Add(csvReader.GetDecimal(10));
                //trips.extra.Add(csvReader.GetDecimal(11));
                //trips.mta_tax.Add(csvReader.GetDecimal(12));
                //trips.tip_amount.Add(csvReader.GetDecimal(13));
                //trips.tolls_amount.Add(csvReader.GetDecimal(14));
                //trips.improvement_surcharge.Add(csvReader.GetDecimal(15));
                //trips.total_amount.Add(csvReader.GetDecimal(16));
                i++;

                if (i >= 1000)
                {
                    WriteRowGroup(destination, trips);
                    i = 0;
                    trips = new Trips();
                }
            }


            return i;
        }

        private static void WriteRowGroup(string destination, Trips trips)
        {
            //create data columns with schema metadata and the data you need
            var vendorId = new DataColumn(
                new DataField<int>("VendorID"),
                trips.VendorID.ToArray());

            //var pickup = new DataColumn(
            //    new DataField<DateTime>("tpep_pickup_datetime"),
            //    trips.tpep_pickup_datetime.ToArray());

            // create file schema
            var schema = new Schema(vendorId.Field);

            using Stream fileStream = System.IO.File.OpenWrite(destination);
            using var parquetWriter = new ParquetWriter(schema, fileStream);
            // create a new row group in the file
            using ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup();
            groupWriter.WriteColumn(vendorId);
            //groupWriter.WriteColumn(pickup);
        }

        public static void ToSqlServer(string sqlConn, string blobConn)
        {
            using SqlConnection conn = new (sqlConn);

            conn.Open();
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select top 0 * from dbo.trips";
            var reader = cmd.ExecuteReader();
            var tableSchema = reader.GetColumnSchema();

            var options = 
                new CsvDataReaderOptions { 
                    Schema = new CsvSchema(tableSchema)
                };

            var blob = new BlobTests().GetBlob(blobConn);

            var blobOption = new BlobOpenReadOptions(false) { BufferSize = 40 };
            using var sr = new StreamReader(blob.OpenRead(blobOption));

            using var csv = CsvDataReader.Create(sr, options);

            using SqlConnection conn2 = new (sqlConn);
            var loader = new BulkSqlLoader(sqlConn, "dbo.trips");
            loader.LoadData(csv);
        }
    }
}
