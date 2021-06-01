package edu.ucr.cs.cs167.vsubr010

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.beast.geolite.Feature.getClass
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext

    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      val tweets = sparkContext.readCSVPoint("tweets_ca.csv", delimiter = '\t', skipHeader = true)
      val counties = sparkContext.shapefile("tl_2018_us_county.zip")
      val timezones = sparkContext.shapefile("ne_10m_time_zones.zip")
      //val feature = new Feature()
      /*code to get the coffee count per tweets*/
      val f = tweets.filter(x=> x.getAs[String](2).contains("coffee"))
      val dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     /*code to get coffee count per tweets*/ val cal: Calendar = Calendar.getInstance()
      val time: Date = dateParser.parse(f.getAs[String](1))
      /*code to study the relationship between Coffee and time*/
      val cal: Calendar = Calendar.getInstance(TimeZone.getTimeZone(timezone.getAs[String]("time_zone")))
      cal.setTime(time)
      val hour = cal.get(Calendar.HOUR_OF_DAY)
      val map = hour.map((x,y) => (x,1))
       val h = map.CountByValue()
      /*Code to study the relationship between Coffee and time with timezone*/
      val coffeeTweetsTimeZone: RDD[(IFeature, IFeature)] = h.spatialJoin(timezones)
      /*Code for Donut*/
      val f = tweets.filter(x=> x.getAs[String](2).contains("Donut"))
      val DonutsperTimeZone: RDD[(IFeature, IFeature)] = f.spatialJoin(counties)
      val count = DonutsperTimeZone.map((x,y) => (x,1))
      val totalcount =count.countByValue().sortByKey(false).take(20)
      val totalcountRDD =sparkContext.parallelize(totalcount)
      /*Code for Bagel*/
      val f = tweets.filter(x=> x.getAs[String](2).contains("Bagel"))
      val BagelperTimeZone: RDD[(IFeature, IFeature)] = f.spatialJoin(counties)
      val count = BagelsperTimeZone.map((x,y) => (x,1))
      val totalcount =count.countByValue().sortByKey(false).take(20)
       val totalcountRDD =sparkContext.parallelize(totalcount)
      //  top_counties.plotImage(2000, 2000, "counties_donut.png")
      for (h <- 0 to 23) {
        print(h.getOrElse(h, 0))
        if (h != 23)
          print(", ")



      }



      }

      // Write the output in CSV format
      //finalResults.saveAsCSVPoints(filename = "output", xColumn = 0, yColumn = 1, delimiter = ';')
    }
  //finally {
      sparkSession.stop()
   // }
  }
