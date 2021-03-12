import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import java.net.URL
import scala.util.Try

object data_mart extends App {
  val spark = SparkSession.builder().appName("lab03_yv")
                          .master("yarn")
                          .getOrCreate()

  import spark.implicits._

  val get_host = udf { (url: String) => Try(new URL(url).getHost).toOption}

  val logs_df = spark.read.json("/labs/laba03/weblogs.json").withColumn("visits", explode($"visits"))
    .select("uid", "visits.*")
    .withColumn("host", get_host(col("url")))
    .withColumn("domain", regexp_replace($"host", "^www[\\.]?", ""))
    .drop("url", "host", "timestamp")

   val sites_type =spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/labdata")
    .option("dbtable", "public.domain_cats")
    .option("driver", "org.postgresql.Driver")
    .option("user", "yuri_vasilevskii")
    .option("password", "TtccdFRY")
    .load()

  val visits_with_cats = logs_df.join(broadcast(sites_type), Seq("domain"), "inner")
    .select($"uid", concat(lit("web_"), $"category").alias("web_cat"))
    .groupBy(col("uid")).pivot("web_cat").agg(count("*"))
    .na.fill(0)

  val visits_df = spark.read
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes", "10.0.0.5")
    .option("es.port", "9200")
    .option("es.net.http.auth.user", "yuri.vasilevskii")
    .option("es.net.http.auth.pass", "TtccdFRY")
    .load("visits")
    .filter(col("uid").isNotNull)
    .withColumn("shop_cat", concat(lit("shop_"), regexp_replace(lower($"category"), "[\\s-]+", "_")))
    .select("uid", "shop_cat")
    .groupBy(col("uid")).pivot("shop_cat").agg(count("*"))

  import org.apache.spark.sql.cassandra._

  spark.conf.set("spark.cassandra.connection.host","10.0.0.5")
  spark.conf.set("spark.cassandra.connection.port","9042")
  spark.conf.set("spark.cassandra.auth.username","yuri.vasilevskii")
  spark.conf.set("spark.cassandra.auth.password","TtccdFRY")

  val clients_df = spark.read
    .format("org.apache.spark.sql.cassandra")
    .option("table", "clients")
    .option("keyspace", "labdata")
    .load()
    .withColumn("age_cat",
      when(col("age") >= 18 && col("age") <= 24, "18-24")
        .when(col("age") >= 25 && col("age") <= 34, "25-34")
        .when(col("age") >= 35 && col("age") <= 44, "35-44")
        .when(col("age") >= 45 && col("age") <= 54, "45-54")
        .otherwise(">=55"))
    .drop($"age")

  val res = clients_df.alias("c")
    .join(visits_with_cats.alias("l"), Seq("uid"), "left")
    .join(visits_df.alias("s"), Seq("uid"), "left")
    .drop("l.uid", "s.uid")
    .na.fill(0)

  res.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.5:5432/yuri_vasilevskii")
    .option("dbtable", "public.clients")
    .option("driver", "org.postgresql.Driver")
    .option("user", "yuri_vasilevskii")
    .option("password", "TtccdFRY")
    .mode("overwrite")
    .save

  spark.stop()
}
