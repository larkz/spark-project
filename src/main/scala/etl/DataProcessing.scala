package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProcessing {
    println("larkin")
    val spark = SparkSession.builder().getOrCreate()

    def getParquet(parquetPath: String): DataFrame = {
        val spark = SparkSession.builder().getOrCreate()
        println("read parquet!")
        spark.read.parquet(parquetPath)
    }

    def readCSV(csvPath: String): DataFrame = {
        println("read csv!")
        val csvPath = "/Users/eric/Dropbox/SharpestMinds/Gaurang/Mentorship/instacart_prediction/data/order_products__prior.csv"
        val csvDf = spark.read.format("csv").option(
            "header", "true").option(
            "inferSchema", "true").load(
            csvPath
        )
        csvDf
    }

    // "⁨/Users/eric/Dropbox/SharpestMinds/Gaurang/Mentorship/instacart_prediction/data/order_products__prior.csv"

    def writeToCSV(df: DataFrame, fileName: DataFrame): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        println("writing dataframe to csv!")
        df.write.format("com.databricks.spark.csv").option("header", "true").save("mydata.csv")
    }

}