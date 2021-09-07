using System;
using System.Collections.Generic;

namespace AzureSQL.LoadTests
{
    public class Trips
    {
        public List<int> VendorID = new ();

        public List<DateTime> tpep_pickup_datetime = new();

        public List<DateTime> tpep_dropoff_datetime = new();

        public List<int> passenger_count = new();

        public List<decimal> trip_distance = new();

        public List<int> RatecodeID = new();

        public List<char> store_and_fwd_flag = new();

        public List<int> PULocationID = new();

        public List<int> DOLocationID = new();

        public List<int> payment_type = new();

        public List<decimal> fare_amount = new();

        public List<decimal> extra = new();

        public List<decimal> mta_tax = new();

        public List<decimal> tip_amount = new();

        public List<decimal> tolls_amount = new();

        public List<decimal> improvement_surcharge = new();

        public List<decimal> total_amount = new();
    }
}
