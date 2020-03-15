import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object process_csv
{
  def main(args: Array[String])
  {
    val spark = SparkSession
    .builder()
    .appName("Process raw data")
    .getOrCreate()

    // S3 file paths
    val sourceFilePath = "s3://s3-loan-stream/raw_data/raw_data.csv"
    val firstS3Path = "s3://s3-loan-stream/report_one/case_one_example_output.csv"
    val secondS3Path = "s3://s3-loan-stream/report_two/case_two_example_output.csv"

    // Read Source data from S3/raw_data path
    val sourceDf = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(sourceFilePath)

    // Question 1 base table
    val incomeLoanDf = sourceDf.select("annual_inc", "loan_amnt", "term")
    .coalesce(24)
    // It can be persisted for later use
    //.persist()

    // Question 2 base table
    val creditBaseDf = sourceDf
    .filter(
      (col("loan_amnt") > 1000.0) &&
      (col("funded_amnt") >= col("loan_amnt"))
    )
    .select("loan_amnt", "funded_amnt",
      "grade", "last_pymnt_amnt")
    // After filter operation data can be skewed
    .coalesce(24)
    // It can be persisted for later use
    //.persist()

    // Drop source table, no need to cache
    sourceDf.unpersist(true)

    // Term string replace as '36 months' -> '36'
    val incomeDf = incomeLoanDf
    .withColumn("term_val",
    regexp_replace(col("term") , lit(" months"), lit("")).as("term"))

    // Define annual income ranges
    val groupedByAnnualDf =
      incomeDf.withColumn("income_range",
        when(col("annual_inc") < 40000, lit("<40k"))
        .when(col("annual_inc") < 60000, lit("40-60k"))
        .when(col("annual_inc") < 60000, lit("60-80k"))
        .when(col("annual_inc") < 60000, lit("80-100k"))
        .otherwise(lit(">100k"))
      )

    // Create result table for question one
    val reportOneDf = groupedByAnnualDf.groupBy(col("income_range"))
    .agg(avg("loan_amnt"), avg("term_val"))
    // Result is too small, no need to distrubute
    .coalesce(1)

    // Save first result table to s3  in csv format
    reportOneDf.write.format("csv").option("header","true").mode("Overwrite")
    .save(firstS3Path)

    // Amount rate calculation
    val creditGradeDf = creditBaseDf.withColumn("amount_rate_double",
      col("last_pymnt_amnt") / col("funded_amnt") * 100
    )

    // Amount rate string replace and prepare final table
    val paidRatioDf = creditGradeDf.withColumn("fully_paid_amount_rate",
      concat(col("amount_rate_double").cast("String"), lit("%"))
    )
    .select("grade", "fully_paid_amount_rate")
    .coalesce(1)

    // Save second result table to s3 in csv format
    paidRatioDf.write.format("csv")
    .option("header","true")
    .mode("Overwrite")
    .save(secondS3Path)

    // Stop spark session
    spark.stop()


  }
}
