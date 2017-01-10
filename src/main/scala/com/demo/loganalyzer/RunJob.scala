package com.demo.loganalyzer

import org.apache.spark._
import org.joda.time.DateTime

/**
  *
  * Created by Sreejith Pillai.
  *
  **/
object RunMainJob extends TransformMapper with Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-loganalyzer")
    val sc = new SparkContext(conf)
    val startTimeJob = new DateTime(sc.startTime)
    val applicationId = sc.applicationId
    log.info("Application launched with id : " + applicationId)

    val rawData = sc.textFile("/user/spillai/testlog")
    val numberOfRawLines = rawData.count()
    log.info("Number of lines to parse : " + numberOfRawLines)

    val mapRawData = MapRawData
    val parseData = rawData.flatMap(x => mapRawData.mapRawLine(x))
    log.info("Number of lines after parsing: ")

    transform(parseData)
  }
}
