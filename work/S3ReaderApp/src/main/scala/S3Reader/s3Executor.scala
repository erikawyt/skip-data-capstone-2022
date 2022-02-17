package S3Reader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.monotonically_increasing_id

//  ref https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/
case class Test(key:String, value:String)
object s3Executor {
  def execute(args: Array[String]): Unit = {
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
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("google.cloud.auth.service.account.email", System.getenv("ACCOUNT_EMAIL"))
    spark.conf.set("google.cloud.auth.service.account.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    import spark.implicits._
    val s3RDD = spark.sparkContext.textFile(s3Path)

    val rdd_new = s3RDD
      .filter(f => !f.equals("{") && !f.equals("}")).map(x => x.split(":"))
      .map(x => Test(x(0).strip(),x(1).replace(",","")))

    val rdd1 = rdd_new.filter(x => x.key.equals("\"order_id\"")).map(x => x.value)
    val rdd2 = rdd_new.filter(x => x.key.equals("\"custo_id\"")).map(x => x.value)
    val rdd3 = rdd_new.filter(x => x.key.equals("\"order.item_grp_id\"")).map(x => x.value)
    val rdd4 = rdd_new.filter(x => x.key.equals("\"order.qty\"")).map(x => x.value)
    val allRDDs = Seq(rdd1, rdd2, rdd3, rdd4)
    val res = applyFuncToZip(allRDDs, (s: Seq[String]) => s.toString())

    res.foreach(s => println(s)) // TODO: construct an object with right format and save to gcs instead of println
  }

  // zip the RDDs into an RDD of Seq[Int]
  def makeZip(s: Seq[RDD[String]]): RDD[Seq[String]] = {
    if (s.length == 1)
      s.head.map(e => Seq(e))
    else {
      val others = makeZip(s.tail)
      val all = s.head.zip(others)
      all.map(elem => Seq(elem._1) ++ elem._2)
    }
  }

  // zip and apply arbitrary function from Seq[Int] to Int
  def applyFuncToZip(s: Seq[RDD[String]], f:Seq[String] => String): RDD[String] = {
    val z = makeZip(s)
    z.map(f)
  }

  def main(args: Array[String]): Unit = {
    // define commons, best to get credentials from environment variables
    val accessKeyID = System.getenv("ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("SECRET_ACCESS_KEY")
    val s3Path = "s3a://skip-capstone-2022/grocery_order_transaction_data/*.json"
    execute(Array(accessKeyID,
      secretAccessKey,
      s3Path
    ))
  }
}
