package etl

import org.apache.spark.sql.SparkSession

object IngestParquetFromCsv {
    val spark = SparkSession.builder().getOrCreate()

    def ingest(path: String, outputPath: String): Unit = {
        val df = spark.read.csv(path)
        df.show
        println(df.count)
        spark.write.parquet(outputPath)
        
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