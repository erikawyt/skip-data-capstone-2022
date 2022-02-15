package S3Reader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.DataFrame

//  ref https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/
case class Test(key:String, value:String)
object s3Executor {
  def execute(args: Array[String]): RDD[String] = {
    import org.apache.spark.sql.SparkSession
    val accessKeyID = args(0) //  accessKeyID
    val secretAccessKey = args(1) //  secretAccessKey
    val s3Path = args(2) // s3Path
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", accessKeyID)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    import spark.implicits._
    val s3RDD = spark.sparkContext.textFile(s3Path)
    val df = s3RDD.map(x => x.split(":")).map(x => Test(x(0),x(0))).toDF()
    df.show()
    df.printSchema()
    // val s3RDD = spark.sparkContext.textFile(s3Path).map(_.split("\n")).flatMap(x => x) // .filter(line => line.equals("{") || line.equals("}")
    s3RDD
  }

  def main(args: Array[String]): Unit = {
    // define commons, best to get credentials from environment variables
    val accessKeyID = System.getenv("ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("SECRET_ACCESS_KEY")
    val s3Path = "s3a://skip-capstone-2022/grocery_order_transaction_data/*.json"
    val s3ResultRDD = execute(Array(accessKeyID,
      secretAccessKey,
      s3Path
    ))
    s3ResultRDD.take(3).foreach(f => println(f))

//    s3ResultRDD.write
//    s3ResultRDD.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.enable", "true")
//    s3ResultRDD.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.email", "Your_service_email")
//    s3ResultRDD.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.keyfile", "path/to/your/files")
  }
}