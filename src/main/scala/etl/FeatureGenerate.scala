package etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object FeatureGenerate {

    def getParquet(parquetPath: String): DataFrame = {
        
        val spark = SparkSession.builder().getOrCreate()
        println("read parquet!")
        spark.read.parquet(parquetPath)
    }

    def pastTime(parquetPath: String): DataFrame = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val toInt = udf[Int, String]( _.toInt)
        
        val sampleData = spark.read.option("header", "true").csv(
            "data/train.csv").withColumn(
            "hour_int", toInt($"hour"))
        
        // Calculating historical average of CTR
        // Split data into previous and current time

        sampleData.select("hour_int").describe().show
        val pastData = sampleData.filter($"hour_int" < 14102100 + 300)
        val currentData =  sampleData.filter($"hour_int" >= 14102100 + 300)

        val pastDataSub = pastData.select("id", "click", "banner_pos").withColumn("click", toInt($"click")) 

        // Get number of clicks per banner_pos
        val clickCount = pastDataSub.groupBy($"banner_pos".as("banner_pos_feature")
            ).agg(sum("click").alias("clicks"), 
            count("id").alias("impressions")
            ).withColumn("banner_ctr", $"clicks"/$"impressions")

        val currentDataWithCTRFeatures = currentData.join(
            clickCount, 
            currentData("banner_pos") === clickCount("banner_pos_feature"), 
            "left_outer")

    }


}