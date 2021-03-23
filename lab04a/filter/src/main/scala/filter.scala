import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StringType, LongType, IntegerType}
import org.apache.spark.sql.functions.{from_json, date_format, col, udf}
import java.util.Date
import java.util.TimeZone

object filter extends App{
  val spark = SparkSession.builder().appName("lab04_yv").getOrCreate()
  import spark.implicits._

  val topic_name = spark.conf.get("spark.filter.topic_name")
  val offset = if (spark.conf.get("spark.filter.offset") == "earliest") -2 else spark.conf.get("spark.filter.offset")
  val output_dir_prefix = spark.conf.get("spark.filter.output_dir_prefix")

  val schema = new StructType()
    .add("event_type",StringType)
    .add("category",StringType)
    .add("item_id",StringType)
    .add("item_price",IntegerType)
    .add("uid",StringType)
    .add("timestamp",LongType)

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> s"$topic_name",
    "startingOffsets" -> s""" { "$topic_name": { "0": $offset } } """
  )

  val getDate = udf {(timestamp : Long) =>
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(new Date(timestamp))}

  val df = spark
    .read.format("kafka")
    .options(kafkaParams)
    .load
    .select('value.cast("string")).as[String]
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    .withColumn("p_date", date_format(getDate(col("timestamp")), "yyyyMMdd"))

  df.filter(col("event_type") === "view")
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$output_dir_prefix/view")

  df.filter(col("event_type") === "buy")
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$output_dir_prefix/buy")

  spark.stop()
}
