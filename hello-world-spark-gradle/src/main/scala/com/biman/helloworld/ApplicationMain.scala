package com.biman.helloworld

import org.apache.spark.sql.SparkSession

object ApplicationMain {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("Hello World")
        .master("local[*]")
        .getOrCreate();

    val listOfWords =
      List("Today is a rainy day",
        "Yesterday was a windy day",
        "Tomorrow will be a sunny day");

    val linesRdd = spark.sparkContext.parallelize(listOfWords)

    val wordsRdd = linesRdd.flatMap(line => line.split(" "))

    val wordTupleRdd = wordsRdd.map(word => (word, 1))

    val wordCountMap = wordTupleRdd.countByKey()

    wordCountMap.foreach(println)

  }

}
