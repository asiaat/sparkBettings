package com.koproj.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io._
import scala.io.Source

/**
  * Utils for spark tasks
  */
class MoneyConv() extends Serializable  {

  // map for currencies
  var mapCur:Map[String,Float] = Map()

  // Convert given currency to EUR
  def toEUR( a:String, b:Float) : Float = b / mapCur(a)

  // Read currency rates into map
  def readCSV() = {

    for (line <- Source.fromFile("input/currency.csv").getLines.drop(1)) {
      val cols = line.split(",").map(_.trim)

      mapCur += (cols(0).toString -> cols(1).toFloat)

    }

  }

}

/*
 *
 */
class OutputMngr() extends Serializable  {

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}



