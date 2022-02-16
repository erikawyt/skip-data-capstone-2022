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

    val order_id = rdd_new.filter(x => x.key.equals("\"order_id\"")).map(x => x.value).toDF()
      .withColumnRenamed("value", "OrderID")
      .withColumn("row_id", monotonically_increasing_id())
    val custom_id = rdd_new.filter(x => x.key.equals("\"custo_id\"")).map(x => x.value).toDF()
      .withColumnRenamed("value", "CustomerID")
      .withColumn("row_id", monotonically_increasing_id())
    val item_id = rdd_new.filter(x => x.key.equals("\"order.item_grp_id\"")).map(x => x.value).toDF()
      .withColumnRenamed("value", "ItemGrpID")
      .withColumn("row_id", monotonically_increasing_id())
    val quantity = rdd_new.filter(x => x.key.equals("\"order.qty\"")).map(x => x.value).toDF()
      .withColumnRenamed("value", "Qty")
      .withColumn("row_id", monotonically_increasing_id())

    val finalDF = order_id.join(
      custom_id.join(
        item_id.join(
          quantity, "row_id"),
        "row_id"),
      ("row_id"))
      .drop("row_id")
    finalDF.write.option("path", "gs://{bucketname}//first_table_test").save()
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
