package eng.test.solution

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.expressions.Window

object UserSongSessionStats {

  val SessionLagMinutes = 20
  val TopUserSessionCount = 50
  val TopSongCount = 10

  //Assumed that fields are nullable, no further information is given about the schema
  val Schema:StructType = new StructType()
    .add("userid",StringType, nullable = true)
    .add("timestamp",TimestampType,nullable = true)
    .add("musicbrainz-artist-id",StringType,nullable = true)
    .add("artist-name",StringType, nullable = true)
    .add("musicbrainz-track-id",StringType,nullable = true)
    .add("track-name",StringType,nullable = true)

  def main(args: Array[String]): Unit = {

    // We could apply validations to file input file, like checking if exists, it's not empty and so forth for failing fast
    val sourceFile = args(0)
    val outputFile = args(1)

    // create Spark context with Spark configuration
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Song Stats")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val songStats = readTsvWithSession(spark, sourceFile, Schema)

      val songStatNormalized = getCleanedAndValidDataset(songStats)

      val top10Songs =
        calculateTopSongsForSessionsInDataset(
          songStatNormalized,
          SessionLagMinutes, //--> Window lag for sessionization
          10, //--> Top 10 Songs
          50 //--> Over top 50 longest sessions
        )

      saveResults(top10Songs,outputFile)
    }
    finally {
      spark.stop()
    }
  }

  /**
   * Returns a new Dataset with deduplication and filtering on required attributes for metrics calculation was performed.
   * A row is considered valid if required columns: userid, timestamp, artist-name and track-name are present.
   * Additionally column names are normalized for following camel case patterns
   *
   * @param songStatsDF Raw song stats dataset
   * @return A new [[org.apache.spark.sql.DataFrame]] with valid rows
   */
  def getCleanedAndValidDataset(songStatsDF:DataFrame): DataFrame = {
    // In a production pipeline we will perform a more robust cleansing and validation steps for
    // segregating valid and invalid rows.
    // For the purpose of showing this in the exercise, as an example, I will apply a distinct for removing duplicates
    // and will consider valid rows the ones that have non null values on fields used during processing
    songStatsDF
      //Normalize column names
      .withColumnRenamed("userid", "userId")
      .withColumnRenamed("artist-name", "artistName")
      .withColumnRenamed("track-name", "trackName")
      //Remove unused columns
      .drop("musicbrainz-artist-id")
      .drop("musicbrainz-track-id")
      .where(
        col("userId").isNotNull && col("timestamp").isNotNull &&
          col("artistName").isNotNull && col("trackName").isNotNull)
      .distinct()
  }

  /**
   * Returns a new Dataset with the top topSongCount songs using the top topSessionCount with the specified
   * lag session lagSessionMin minutes.
   *
   * @param songStatNormalized Raw song stats dataset
   * @param lagSessionMin Session lag duration in minutes
   * @param topSongCount Top song count
   * @param topSessionCount Top session count
   * @return A new [[org.apache.spark.sql.DataFrame]] with the resultset
   */
  def calculateTopSongsForSessionsInDataset(
                                             songStatNormalized: DataFrame,
                                             lagSessionMin: Int,
                                             topSongCount: Int,
                                             topSessionCount: Int): DataFrame = {
    val userSessions = sessionizeByUserAndTimestamp(songStatNormalized, lagSessionMin).cache()
    val topLongestSessions = calculateTopLongestSessions(userSessions, topSessionCount)
    calculateTopSongsForSessions(userSessions, topLongestSessions, topSongCount)
  }

  /**
   * Returns a new Dataset with the result of the sessionization of the user data using lagMaxTimeMin as threshold
   *
   * @param songDF Raw song stats dataset
   * @param lagMaxTimeMin Threshold in minutes for sesison lag
   * @return A new [[org.apache.spark.sql.DataFrame]] with sessionized rows
   */
  def sessionizeByUserAndTimestamp(songDF: DataFrame, lagMaxTimeMin: Int): DataFrame = {
    val windowSpec = Window.partitionBy("userid").orderBy("timestamp")

    songDF
      .withColumn("previousTimestamp", lag("timestamp",1).over(windowSpec))
      .withColumn(
        "previousLagMin",
        round(
          (unix_timestamp(col("timestamp")) - unix_timestamp(coalesce(col("previousTimestamp"), col("timestamp")))) / 60,
          2)
        )
      .withColumn("sessionCnt", sum(when(col("previousLagMin") >= lagMaxTimeMin, 1).otherwise(0)).over(windowSpec))
      .withColumn("sessionId",concat(col("userId"),lit("_session_"), col("sessionCnt")))
  }

  /**
   * Returns a new Dataset with the top longest sessions limited by topSessionCount
   *
   * @param userSessionsDF Sesionized song dataset
   * @param topSessionCount Top song count
   * @return A new [[org.apache.spark.sql.DataFrame]] with the resultset
   */
  def calculateTopLongestSessions(userSessionsDF:DataFrame, topSessionCount: Int): Dataset[Row] = {
    userSessionsDF
      .groupBy("sessionId")
      .agg((unix_timestamp(max(col("timestamp"))) - unix_timestamp(min(col("timestamp")))).as("sessionDuration"))
      .orderBy(desc("sessionDuration"))
      .limit(topSessionCount)
  }


  /**
   * Returns a new Dataset with the top longest sessions limited by topSessionCount
   *
   * @param sessionizedSongDF Sesionized song dataset
   * @param topSessionsDF Top session dataset
   * @param topSongCount Top song count
   * @return A new [[org.apache.spark.sql.DataFrame]] with the resultset
   */
  def calculateTopSongsForSessions(sessionizedSongDF:DataFrame, topSessionsDF:DataFrame, topSongCount: Int): Dataset[Row] = {
    sessionizedSongDF
      .join(topSessionsDF, sessionizedSongDF.col("sessionId") === topSessionsDF.col("sessionId"), "leftsemi")
      .groupBy("artistName", "trackName")
      .agg(count("trackName").as("trackCnt"))
      .orderBy(desc("trackCnt"), asc("artistName"), asc("trackName"))
      .limit(topSongCount)
  }

  /**
   * Saves a dataset to the specified location using CSV format delimited by '\t'
   *
   * @param dataFrame Dataframe reference
   * @param outputFile File location
   */
  def saveResults(dataFrame: DataFrame, outputFile: String): Unit = {
    dataFrame
      //I'm repartition in order to force spark to write one file and make the load simpler for the verification
      //of course this is not recommended for prod
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("delimiter", "\t")
      .option("header", "true")
      .csv(outputFile)
  }

  /**
   * Returns a Dataset using the provided spark session with the data available in the specified location.
   * This method expects to read a CSV file delimited by '\t', without header and without multi-lines
   *
   * @param sessionizedSongDF Sesionized song dataset
   * @param topSessionsDF Top session dataset
   * @param topSongCount Top song count
   * @return A new [[org.apache.spark.sql.DataFrame]] with the resultset
   */
  def readTsvWithSession(spark: SparkSession, inputFile:String, schema: StructType): DataFrame = {
    spark
      .read
      .format("csv")
      .option("header", "false")
      .option("delimiter", "\t")
      .schema(schema)
      .load(inputFile)
  }
}
