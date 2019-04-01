package com.koproj.scala

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Level
import org.apache.log4j.Logger

object CountryProfit {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CountryProfit.scala")
    val sc = new SparkContext(conf)

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
    val rddTxConv = rddTxFields.map(f => (f._1,(convertCur(f._2,f._3.toFloat),f._4 )))

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

    // union
    val profit = CountryBetTaxSum.union(CountryWinSum).reduceByKey((x,y) => x - y)
    profit.saveAsTextFile("output/profit.csv")




  }
}