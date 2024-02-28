import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}

object Main {

  val givenSize = 3

  // peerid, id1, id2, year
  val sampleData = Seq(
    ("ABC17969(AB)", "1", "ABC17969", 2022),
    ("ABC17969(AB)", "2", "CDC52533", 2022),
    ("ABC17969(AB)", "3", "DEC59161", 2023),
    ("ABC17969(AB)", "4", "F43874", 2022),
    ("ABC17969(AB)", "5", "MY06154", 2021),
    ("ABC17969(AB)", "6", "MY4387", 2022),
    ("AE686(AE)", "7", "AE686", 2023),
    ("AE686(AE)", "8", "BH2740", 2021),
    ("AE686(AE)", "9", "EG999", 2021),
    ("AE686(AE)", "10", "AE0908", 2021),
    ("AE686(AE)", "11", "QA402", 2022),
    ("AE686(AE)", "12", "OM691", 2022)
  )


  def processData(spark: SparkSession,df: DataFrame, givenSize: Int): DataFrame = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    import spark.implicits._

    // 1.	For each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.
    val topYearDF = df
      .filter($"peer_id".contains($"id_2"))                       // filter record which peer_id contains id2
      .groupBy("peer_id")                                  // group the result by peerId
      .agg(max("year").as("top_year"))        // output top year to console

    println("1.\tFor each peer_id, get the year when peer_id contains id_2, for example for ‘ABC17969(AB)’ year is 2022.")
    topYearDF.show()

    // 2.	Given a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).
    val countDF = df
      .join(topYearDF, "peer_id")                  // using first result df
      .where($"year" <= $"top_year")                 // filter records <= top year 2022
      .groupBy("peer_id", "year")                  // group
      .agg(count("year").as("count"))       // count the year records
      .orderBy("peer_id", "year")             // output

    println("2.\tGiven a size number, for example 3. For each peer_id count the number of each year (which is smaller or equal than the year in step1).")
    countDF.show()

    // 3.A Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.
    val windowSpec = Window.partitionBy("peer_id").orderBy($"year".desc)
    val countSumDF = countDF
      .withColumn("sum_count", sum("count").over(windowSpec))          // loop sorted records via year, and sum total from current records count
      .filter($"sum_count" >= givenSize)                                           // filter by given size, ex: 2023 only hv 1 . so check if it match case B

    println("3.A Order the value in step 2 by year and check if the count number of the first year is bigger or equal than the given size number. If yes, just return the year.")
    countSumDF.show()

    // 3.B If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number.
    // As 1(2023 count) + 2(2022 count) >= 3 (given size number), the output would be 2023, 2022.
    val minYearDF = countSumDF
      .groupBy("peer_id")                                                             // group by pear ID for only 1 return
      .agg(min("sum_count").as("min_sum_count"))                         // new alias for 2 df join
      .join(countSumDF, "peer_id")                                              // join self to retrieve the year
      .where($"sum_count" === $"min_sum_count")                                   // get same record
      .select($"peer_id", $"year".as("min_year"))                               // select and return
      .distinct()


    val finalResult = countDF                                                               // rejoin
      .join(minYearDF, "peer_id")
      .where($"year" >= $"min_year")
      .select($"peer_id", $"year")
      .orderBy($"peer_id", $"year".desc)

    println("3.B If not, plus the count number from the biggest year to next year until the count number is bigger or equal than the given number.")
    finalResult
  }

  def main(args: Array[String]): Unit = {
    // Logger.getLogger("org").setLevel(Level.WARN)
    // Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession.builder
      .appName("YearCountAnalysis")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df = sampleData.toDF("peer_id", "id_1", "id_2", "year")
    val result = processData(spark,df, givenSize)
    result.show(false)

    spark.stop()
  }
}






