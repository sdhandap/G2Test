package com.assignment.main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sdhandapani on 1/25/2017.
  */
object SparkSingletons {
  var sparkConf: SparkConf = new SparkConf()
  var sparkContext: SparkContext = new SparkContext(sparkConf)
  var sqlContext: SQLContext = new SQLContext(sparkContext)
}
