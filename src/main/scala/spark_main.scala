package spark_main


object spark_main {


  def main(args: Array[String]): Unit = {
    val dummy = args(0)
    
    dummy match {
      case "job1" => job1.job1sub.method1()
    }
  }
}
  