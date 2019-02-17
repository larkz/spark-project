package job1

import org.apache.spark.sql.SparkSession

object job1sub {
    val spark = SparkSession.builder().getOrCreate()
    // Change pretend

    println("larkin")
    def method1(): Unit = {
        val df = spark.read.csv("data/FireDepartmentSample.csv")
        df.write.parquet("data/Fire.parquet")
    }
}