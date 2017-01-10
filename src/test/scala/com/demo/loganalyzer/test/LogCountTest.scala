package com.demo.loganalyzer.test

import com.demo.loganalyzer.{MapRawData, LogSchema}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, FunSuite}

/**
  * Created by Sreejith Pillai.
  */
class LogCountTest extends FunSuite with Matchers {

  private val hadoopPath = System.getProperty("user.dir") + "\\target\\winutils"
  System.setProperty("hadoop.home.dir", hadoopPath)

  @transient var sparkContext: SparkContext = _

  private val sparkConf = new SparkConf()
    .setAppName("SparkFunSuite")
    .setMaster("local")

  def sparkTest(name: String)(body: => Unit): Unit = {
    test(name) {
      sparkContext = new SparkContext(sparkConf)
      try {
        body
      } finally {
        sparkContext.stop()
        sparkContext = null
      }
    }
  }

  private val logInput = "64.242.88.10,[07/Mar/2004:16:05:49 -0800],GET /mailman/listinfo/hsdivision HTTP/1.1 200 6291"
  private val parsedLog = LogSchema("64.242.88.10", "16", Some("GET /mailman/listinfo/hsdivision HTTP/1.1 200 6291"))

  sparkTest("Match") {
    MapRawData.mapRawLine(logInput) should equal(Some(parsedLog))
  }
}
