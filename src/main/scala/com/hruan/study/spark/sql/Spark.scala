package com.hruan.study.spark.sql

import org.apache.spark.sql.{Dataset, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.ParserInterface

object Spark {
  def main(args: Array[String]): Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionBuilder = SparkSessionExtensions => Unit
    val parserBuilder: ParserBuilder = (_, parser) => new StrictParser(parser)
    val extensionBuilder: ExtensionBuilder = { e => e.injectParser(parserBuilder) }

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .withExtensions(extensionBuilder)
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Person("Michael", 29, "CA") :: Person("Andy", 30, "NY")
      :: Person("Justin", 19, "CA") :: Person("Justin", 25, "CA") :: Nil)
    val dataFrame = spark.createDataFrame(rdd, Person.getClass)

    dataFrame.createOrReplaceTempView("people")

    spark.sql("select * from people").show()

    spark.close()
  }
}

case class Person(name: String, age: Int, state: String)
