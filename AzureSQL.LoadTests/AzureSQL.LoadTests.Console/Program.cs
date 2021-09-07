using System.Diagnostics;

namespace AzureSQL.LoadTests.Console
{
    public class Program
    {
        static void Main(string[] args)
        {
            var timer = new Stopwatch();

            //timer.Start();
            //var count = ReadFileTests.StreamReaderCount("C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.csv");
            //timer.Stop();
            //System.Console.WriteLine(count + " in: " + timer.ElapsedMilliseconds);

            //timer.Reset();
            //timer.Start();
            //var count2 = ReadFileTests.CsvHelper("C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.csv");
            //timer.Stop();
            //System.Console.WriteLine(count2 + " in: " + timer.ElapsedMilliseconds);

            //timer.Reset();
            //timer.Start();
            //var count3 = ReadFileTests.Sylvan("C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.csv");
            //timer.Stop();
            //System.Console.WriteLine(count3 + " in: " + timer.ElapsedMilliseconds);

            //timer.Reset();
            //timer.Start();
            //var count4 = ReadFileTests.ToParquet(
            //    "C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.csv",
            //    "C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.parquet");
            //timer.Stop();
            //System.Console.WriteLine(count4 + " in: " + timer.ElapsedMilliseconds);

            timer.Reset();
            timer.Start();
            ReadFileTests.ToSqlServer(
                "C:\\Users\\kelvi\\Documents\\WorkDocs\\yellow_tripdata_2018-01.csv",
                "");
            timer.Stop();
            System.Console.WriteLine(timer.ElapsedMilliseconds);

        }
    }
}
