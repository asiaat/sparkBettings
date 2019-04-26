package com.koproj.scala

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io._

import com.koproj.scala.MoneyConv


object CountryProfit {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CountryProfit.scala")
    val sc = new SparkContext(conf)

    val header_tx     = "transaction_id,customerid,currency,amount,transaction_type"
    val dataTx        = sc.textFile("input/transactions.csv").filter(l => l != header_tx ).distinct()
    val rddTxFields   = dataTx.map(r => r.split(",")).map(f => (f(1),f(3),f(4).toFloat,f(5)))

    val header_cur    = "currency_symbol,currency_rate"
    val dataCur       = sc.textFile("input/currency.csv").filter(l => l != header_cur )
    val rddCurFields  = dataCur.map(r => r.split(",")).map(v => (v(0),v(1).toFloat))

    val mapCur = rddCurFields.collect().toMap

    // whole data with converted currency
    val moncv = new MoneyConv()
    moncv.readCSV()

    val rddTxConv = rddTxFields.map(f => (f._1,(moncv.toEUR(f._2,f._3.toFloat),f._4 )))

    // customer and country
    val customer_header = "customerid,hashed_name,registration_date,country_code"
    val rddCustomer = sc.textFile("input/customers.csv").filter(
      l => l != customer_header).map(r => r.split(","))

    val customerCountry   = rddCustomer.map(f => (f(0),f(3)))

    // join country info to tx rdd
    val joinCountry = rddTxConv.join(customerCountry)

    val rddCountryBet = joinCountry.filter(f => f._2._1._2 == "bet" )
    val tax = 0.01
    val CountryBetTaxSum = rddCountryBet.map(b => (b._2._2,b._2._1._1 - b._2._1._1*tax)).reduceByKey((x,y) => x + y)

    val rddCountryWin = joinCountry.filter(f => f._2._1._2 == "win" )
    val CountryWinSum = rddCountryWin.map(b => (b._2._2,b._2._1._1.toDouble)).reduceByKey((x,y) => x + y)

    def rndBy(x: Double) = {
      val w = math.pow(10, 4)
      (x * w).toLong.toDouble / w
    }

    // union
    val profit = CountryBetTaxSum.union(CountryWinSum).reduceByKey((x,y) => x - y)
    //customerCountry.repartition(1).saveAsTextFile("output/cc1.csv")
    val profitRepartCSV = profit.repartition(1).map{case (key, value) => Array(key, rndBy(value),java.time.LocalDateTime.now).mkString(";")}

    val rddBuf = "tmp/buffer"
    FileUtil.fullyDelete(new File(rddBuf))
    profitRepartCSV.saveAsTextFile(rddBuf)


    /*
     * Merge Hadoop fs to single file
     */
    def merge(srcPath: String, dstPath: String): Unit =  {
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    }

    val destinationCSV= "output/net_country_profit.csv"
    FileUtil.fullyDelete(new File(destinationCSV))

    merge(rddBuf, destinationCSV)

  }
}