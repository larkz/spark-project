package regression

import org.apache.spark.ml.regression.LinearRegression
import etl.DataProcessing
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode

object LinearRegression {

    def bidtime(): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._

        val sampleData = spark.read.option("header", "true").csv("gs://bidtime/train_sample.csv").select("device_conn_type", "C1", "click")
        val sampleDataFeatures = sampleData.withColumn("click_01", sampleData("click").cast(DoubleType)).drop("click").filter(
            $"device_conn_type".isNotNull).filter($"C1".isNotNull).filter($"click_01".isNotNull)
        val dconnIndexer = new StringIndexer().setInputCol(
            "device_conn_type").setOutputCol("device_conn_type_ind")
        val c1Indexer = new StringIndexer().setInputCol(
            "C1").setOutputCol("C1_ind")
        val indexedDf1 = dconnIndexer.fit(sampleDataFeatures).transform(sampleDataFeatures)
        val indexedDf2 = c1Indexer.fit(indexedDf1).transform(indexedDf1)
        val dfModel = indexedDf2.select($"C1_ind", $"device_conn_type_ind", $"click_01" )

        val assembler1 = new VectorAssembler().setInputCols(
            Array("C1_ind", "device_conn_type_ind")).setOutputCol("features")

        val featureDf = assembler1.transform(dfModel).select($"features", $"click_01".as("label"))
        
        val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

        val lrModel = lr.fit(featureDf)

        val predictions = lrModel.transform(featureDf)
        predictions.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save("gs://bidtime/predictions_sample_csv/")

    }
    
    

    ///
    /*
    
    val sampleData = spark.read.option("header", "true").csv("data/FireDepartmentSample.csv")

    val sampleDataSubset = sampleData.select("Call Type Group", 
        "Number of Alarms", 
        "Supervisor District", 
        "Response DtTm", 
        "Dispatch DtTm"
        ).withColumn(
        "response_time", unix_timestamp($"Response DtTm", "MM/dd/yyyy HH:mm:ss").cast(TimestampType)).withColumn(
        "dispatch_time", unix_timestamp($"Dispatch DtTm", "MM/dd/yyyy HH:mm:ss").cast(TimestampType)).withColumn(
        "response_time_sec", unix_timestamp($"response_time")).withColumn(
        "dispatch_time_sec", unix_timestamp($"dispatch_time")).withColumn(
        "net_reaction_time", $"response_time_sec" - $"dispatch_time_sec"
        )
    
    sampleDataSubset.show(false)

    val stringIndexerSupervisorDistrict = new StringIndexer().setInputCol(
        "Supervisor District").setOutputCol("supervisor_district")

    val stringIndexerCallType = new StringIndexer().setInputCol(
        "Call Type Group").setOutputCol("call_type_group")

    val indexedDf1 = stringIndexerSupervisorDistrict.fit(sampleDataSubset).transform(sampleDataSubset).filter(
        $"Call Type Group".isNotNull).filter($"Supervisor District".isNotNull).filter($"net_reaction_time".isNotNull)
    
    val indexedDf2 = stringIndexerCallType.fit(indexedDf1).transform(indexedDf1)

    indexedDf1.select("supervisor_district", "net_reaction_time").show
    indexedDf2.select("supervisor_district", "call_type_group", "net_reaction_time").show

    val assembler1 = new VectorAssembler().setInputCols(
        Array("supervisor_district", "call_type_group")).setOutputCol("features")

    val featureDf = assembler1.transform(indexedDf2).select($"features", $"net_reaction_time".as("label"))
    
    val lrModel = lr.fit(featureDf)

    val trainingSummary = lrModel.summary

    val testData = featureDf.sample(false, 0.05)
    val predictions = lrModel.transform(testData)

    // val training = spark.read.format("libsvm").load("data/test_regression.txt")
    sampleDataSubset.select("Call Type Group").distinct.count
    sampleDataSubset.select("Number of Alarms").distinct.count
    sampleDataSubset.select("Supervisor District").distinct.count
    */
}