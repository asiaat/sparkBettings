package com.koproj.scala

/**
  * Utils for spark tasks
  */

class MoneyConv(m:Map[String,Float]) extends Serializable  {

  // Convert given currency to EUR
  def toEUR( a:String, b:Float) : Float = b / m(a)

}



