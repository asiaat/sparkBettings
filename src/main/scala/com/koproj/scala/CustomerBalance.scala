package com.koproj.scala

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CustomerBalance {
  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CustomerBalance")
    val sc   = new SparkContext(conf)

    val header_tx     = "transaction_id,customerid,currency,amount,transaction_type"
    val dataTx        = sc.textFile("input/transactions.csv").filter(l => l != header_tx )
    val rddTxFields   = dataTx.map(r => r.split(",")).map(f => (f(1),f(3),f(4).toFloat,f(5)))

    val header_cur    = "currency_symbol,currency_rate"
    val dataCur       = sc.textFile("input/currency.csv").filter(l => l != header_cur )
    val rddCurFields  = dataCur.map(r => r.split(",")).map(v => (v(0),v(1)))

    val mapCur = rddCurFields.collect().toMap
    /*
     * Convert currency to EUR according to currency.csv
     */
    def convertCur( a:String, b:Float ) : Float = {
      var conv:Float = 0

      // calculation
      conv = b / mapCur(a).toFloat

      return conv
    }

    // whole data with converted currency
    val rddTxConv = rddTxFields.map(f => (f._1,convertCur(f._2,f._3.toFloat),f._4 ))

    // Filter Tx types
    // and give the direction etc. adding - is reducing balance
    val withdraw   = rddTxConv.filter(f => f._3 == "withdraw").map(w => (w._1,-w._2)).reduceByKey(_ + _)
    val deposit    = rddTxConv.filter(f => f._3 == "deposit").map(w => (w._1,w._2)).reduceByKey(_ + _)
    val bet        = rddTxConv.filter(f => f._3 == "bet").map(w => (w._1,-w._2)).reduceByKey(_ + _)
    val win        = rddTxConv.filter(f => f._3 == "win").map(w => (w._1,w._2)).reduceByKey(_ + _)

    // multiple union
    val multiUnion = sc.union(deposit,withdraw,bet,win).reduceByKey((d,b) => d + b).sortByKey()
    multiUnion.saveAsTextFile("output/credit4.csv")


  }
}