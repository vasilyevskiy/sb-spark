import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StringType, LongType, IntegerType}
import org.apache.spark.sql.functions.{from_json, from_unixtime, col}

object filter extends App{
  val spark = SparkSession.builder().appName("lab04_yv").master("yarn").getOrCreate()
  import spark.implicits._

  val topic_name = spark.conf.get("spark.filter.topic_name")
  val offset = spark.conf.get("spark.filter.offset")
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
    "startingOffsets" -> s"""$offset"""
  )

  val df = spark
    .read.format("kafka")
    .options(kafkaParams)
    .load
    .select('value.cast("string")).as[String]
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")
    .withColumn("p_date", from_unixtime(col("timestamp")/1000,"yyyyMMdd"))

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
