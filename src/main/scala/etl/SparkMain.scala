package etl

object SparkMain {
  
  def main(args: Array[String]): Unit = {
    val dummy = args(0)
    val path =args(1)
    val outputPath =args(1)
    dummy match {
      case "ingest-parquet" => etl.IngestParquetFromCsv.ingest(path, outputPath)
    }
  }
}
  