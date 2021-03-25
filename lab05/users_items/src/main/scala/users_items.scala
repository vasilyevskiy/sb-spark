import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object users_items extends App{
  val spark = SparkSession.builder().appName("lab05_yv").getOrCreate()
  import spark.implicits._

  val mode: Integer = spark.conf.get("spark.users_items.update").toInt
  val input_dir = spark.conf.get("spark.users_items.input_dir")
  val output_dir = spark.conf.get("spark.users_items.output_dir")

  val input_views_df = spark
    .read
    .json(s"$input_dir/view")
    .filter(col("uid").isNotNull)
    .withColumn("item_normalized", concat(lit("view_"),
                regexp_replace(lower($"item_id"), "[\\s-]+", "_")))
    .select($"uid", $"p_date", $"item_normalized")

  val input_buys_df = spark.read
    .json(s"$input_dir/buy")
    .filter(col("uid").isNotNull)
    .withColumn("item_normalized", concat(lit("buy_"),
                regexp_replace(lower($"item_id"), "[\\s-]+", "_")))
    .select($"uid", $"p_date", $"item_normalized")

  val union_df = input_views_df.union(input_buys_df).cache
  union_df.count
  val max_date =  union_df.agg(max("p_date")).head().getInt(0).toString

  val users_x_items = union_df
    .groupBy(col("uid"))
    .pivot("item_normalized")
    .agg(count("*"))
    .na.fill(0)

  if (mode == 0) {
    users_x_items.coalesce(1).write
      .mode("overwrite")
      .parquet(s"$output_dir/$max_date")
  } else {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(new Path(output_dir))
    val path = status(0).getPath().getName()
    val last_matrix = spark.read.parquet(s"$output_dir/$path")
    val result = last_matrix.join(users_x_items,  col("uid"), "left")
    result.coalesce(1)
          .write.mode("overwrite")
          .parquet(s"$output_dir/$max_date")
  }
  union_df.unpersist()
  spark.stop()
}
