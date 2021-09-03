package eng.test.solution

import eng.test.solution.UserSongSessionStats.{Schema, calculateTopSongsForSessionsInDataset, getCleanedAndValidDataset, sessionizeByUserAndTimestamp}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import java.sql.Timestamp
import java.time.Instant

class UserSongSessionStatsSpec extends FlatSpec with BeforeAndAfterEach with Matchers with SparkSessionSetup {

  val outSchema:StructType = new StructType()
    .add("artistName",StringType, nullable = true)
    .add("trackName",StringType,nullable = true)
    .add("trackCnt",IntegerType,nullable = true)

  "Spark" should "not be stopped" in withSparkSession { spark =>
    spark.sparkContext.isStopped should be (false)
  }

  "Dataset loading and cleansing - Filtering invalid and duplicated rows" should "return just valid rows" in withSparkSession { spark =>

    val songList = List(
      Row("user_1", createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", "Some name"),
      // Invalid rows with null data in required columns: userid,timestamp, artist-name, track-name fields
      Row(null, createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", "Some name"),
      Row("user_1", null, "0", "A", "0", "Some name"),
      Row("user_1", createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", null),
      Row("user_1", createTimestamp("2021-09-02T18:12:08Z"), null, null, null, "Some name"),
      // Duplicated rows
      Row("user_2", createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", "Some another song"),
      Row("user_2", createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", "Some another song")
    )

    val songListDF = spark.createDataFrame(
      spark.sparkContext.parallelize(songList),
      Schema
    )

    val expectedList = List(
      Row("user_1", createTimestamp("2021-09-02T18:12:08Z"), "A", "Some name"),
      Row("user_2", createTimestamp("2021-09-02T18:12:08Z"), "A", "Some another song")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedList),
      new StructType()
        .add("userId",StringType, nullable = true)
        .add("timestamp",TimestampType,nullable = true)
        .add("artistName",StringType, nullable = true)
        .add("trackName",StringType,nullable = true)
    )

    val songListCleanedDF = getCleanedAndValidDataset(songListDF)
    songListCleanedDF.count() should be (2)
    songListCleanedDF.except(expectedDF).count() should be (0)
  }

  "Test - Verify user sessionization on close boundaries" should "group session using specified lag" in withSparkSession { spark =>

    val songList = List(
      //Base on a lag of 20 minutes
      //These two should be on the same session
      Row("user_1", createTimestamp("2021-09-02T00:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_1", createTimestamp("2021-09-02T00:19:59Z"), "0", "Art_0", "1", "Song_1"),
      //This other should be on a new session
      Row("user_1", createTimestamp("2021-09-02T00:40:00Z"), "0", "Art_0", "1", "Song_1"),
      //Really out of the lag boundary, should be on a new session
      Row("user_1", createTimestamp("2021-09-02T01:20:00Z"), "0", "Art_0", "1", "Song_1"),
    )

    val songListDF = getCleanedAndValidDataset(
      spark.createDataFrame(
        spark.sparkContext.parallelize(songList),
        Schema
      )
    )

    val expectedList = List(
      Row(createTimestamp("2021-09-02T00:00:00Z"), null, 0),
      Row(createTimestamp("2021-09-02T00:19:59Z"), createTimestamp("2021-09-02T00:00:00Z"), 0),
      Row(createTimestamp("2021-09-02T00:40:00Z"), createTimestamp("2021-09-02T00:19:59Z"), 1),
      Row(createTimestamp("2021-09-02T01:20:00Z"), createTimestamp("2021-09-02T00:40:00Z"), 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedList),
      new StructType()
        .add("timestamp",TimestampType,nullable = true)
        .add("previousTimestamp",TimestampType,nullable = true)
        .add("sessionCnt",IntegerType,nullable = true)
    )

    val userSessionsDF =
      sessionizeByUserAndTimestamp(songListDF, 20)
        .select("timestamp", "previousTimestamp", "sessionCnt")
        .orderBy("userId", "timestamp")

    userSessionsDF.count() should be (4)
    userSessionsDF.except(expectedDF).count() should be (0)
  }

  "Test - Calculate top x songs with empty dataset" should "return an empty DF" in withSparkSession { spark =>

    val songListDF = getCleanedAndValidDataset(spark.createDataFrame(
      spark.sparkContext.parallelize(List[Row]()),
      Schema
    ))

    calculateTopSongsForSessionsInDataset(
      songListDF,
      UserSongSessionStats.SessionLagMinutes,
      10,
      50
    ).count() should be (0)
  }

  "Test - Calculate top x songs most played with single sessions" should "return artist, track and count" in withSparkSession { spark =>

    val songList = List(
      Row("user_1", createTimestamp("2021-09-02T18:12:08Z"), "0", "A", "0", "Some name"),
      Row("user_2", createTimestamp("2021-09-03T20:18:18Z"), "0", "B", "0", "Some another song"),
      Row("user_3", createTimestamp("2021-09-04T01:02:48Z"), "0", "B", "1", "Some another song 2")
    )

    val songListDF = getCleanedAndValidDataset(
      spark.createDataFrame(
        spark.sparkContext.parallelize(songList),
        Schema
      )
    )

    val expectedList = List(
      Row("A", "Some name", 1),
      Row("B", "Some another song", 1),
      Row("B", "Some another song 2", 1)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedList),
      outSchema
    )

    val songListCleanedDF =
      calculateTopSongsForSessionsInDataset(
        songListDF,
        UserSongSessionStats.SessionLagMinutes,
        10,
        50
      )
    songListCleanedDF.count() should be (3)
    songListCleanedDF.except(expectedDF).count() should be (0)
  }

  "Test - Calculate top x songs most played in top y sessions" should "return artist, track and count" in withSparkSession { spark =>
    val songList = List(
      // User activity
      Row("user_1", createTimestamp("2021-09-02T00:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_1", createTimestamp("2021-09-02T00:19:00Z"), "0", "Art_0", "1", "Song_1"), //--> 19m session with Art_0 Song_0, Art_0 Song_1
      //New session for user 2, will filtered by limit session limit
      Row("user_1", createTimestamp("2021-09-02T00:40:00Z"), "0", "Art_0", "1", "Song_1"), //--> 0m session with Art_0 Song_1
      Row("user_2", createTimestamp("2021-09-02T01:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_2", createTimestamp("2021-09-02T01:15:00Z"), "1", "Art_1", "0", "Song_0"), //--> 15m session for with Art_0 Song_0, Art_1 Song_0
      Row("user_3", createTimestamp("2021-09-02T02:00:00Z"), "0", "Art_0", "1", "Song_1"),
      Row("user_3", createTimestamp("2021-09-02T02:10:00Z"), "2", "Art_2", "0", "Song_0"), //--> 10m session Art_0 Song_1, Art_2 Song_0
      Row("user_4", createTimestamp("2021-09-02T00:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_4", createTimestamp("2021-09-02T00:09:00Z"), "0", "Art_0", "1", "Song_1"), //--> 9m session Art_0 Song_0, Art_0 Song_1
      Row("user_5", createTimestamp("2021-09-02T00:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_5", createTimestamp("2021-09-02T00:08:00Z"), "3", "Art_3", "0", "Song_0"), //--> 8m session Art_0 Song_0, Art_3 Song_0
      //This last session will be filtered by session limit
      Row("user_6", createTimestamp("2021-09-02T00:00:00Z"), "0", "Art_0", "0", "Song_0"),
      Row("user_6", createTimestamp("2021-09-02T00:07:00Z"), "2", "Art_2", "0", "Song_0")  //--> 7m session Art_0 Song_0, Art_2 Song_0
    )

    /* Total stats for 5 sessions and top 3 songs
      Art_0, Song_0: 4
      Art_0, Song_1: 3
      Art_1, Song_0: 1
     */

    val songListDF = getCleanedAndValidDataset(spark.createDataFrame(
      spark.sparkContext.parallelize(songList),
      Schema
    ))

    val top3Songs =
      calculateTopSongsForSessionsInDataset(
        songListDF,
        UserSongSessionStats.SessionLagMinutes,
        3,
        5
    )

    val expectedList = List(
      Row("Art_0", "Song_0", 4),
      Row("Art_0", "Song_1", 3),
      Row("Art_1", "Song_0", 1)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedList),
      outSchema
    )

    top3Songs.count() should be (3)
    top3Songs.except(expectedDF).count() should be (0)
  }

  private def createTimestamp(ts: String): Timestamp = {
    Timestamp.from(Instant.parse(ts))
  }
}

trait SparkSessionSetup {
  def withSparkSession(testMethod: SparkSession => Any) {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Song Stats Tests")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      testMethod(spark)
    }
    finally {
      spark.stop()
    }
  }
}

