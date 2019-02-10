package etl

import org.apache.spark.sql.SparkSession

object IngestParquetFromCsv {
    val spark = SparkSession.builder().getOrCreate()

    def ingest(path: String): Unit = {
        val df = spark.read.parquet(path)
        df.show
        print(df.count)
    }

    def main(args: Array[String]): Unit = {
        val parquetInputPath = args(0)
        // val parquetOutputPath = args(1)

        val df = spark.read.parquet(parquetInputPath)
        df.show
        df.count
        // df.write.parquet(parquetOutputPath)
    }
}