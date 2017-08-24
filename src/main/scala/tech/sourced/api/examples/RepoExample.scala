package tech.sourced.api.examples

import org.apache.spark.sql.{DataFrameReader, SparkSession}
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, StringStartsWith}
import tech.sourced.api.RepositoryRelation

import scala.util.Properties

object RepoExample {

  def main(args: Array[String]): Unit = {
    val pathToRepos = "file:///Users/alex/floss/learning-linguist/repos/"
    val sparkMaster = Properties.envOrElse("MASTER", "local[*]")

    val spark = SparkSession.builder()
      .master(sparkMaster)
      .getOrCreate()

    // manual
    new RepositoryRelation(spark.sqlContext, pathToRepos)
      .buildScan(
        Array("repoUrl", "isFork", "UUID", "size"),
        Array(
          StringStartsWith("repoUrl", "https://"),
          EqualTo("isFork", true),
          GreaterThan("size", 1024))
      ).foreach(println)


    val reposDF = spark
      .read.format("git-local")
      .load(pathToRepos)

    // through API
    import org.apache.spark.sql.functions._
    //reposDF.filter(col("isFork") == true).show()

    import spark.implicits._
    //reposDF.filter($"isFork" == true).show()

    reposDF.select("repoURL").show()

    // more advanced
    //val reposDF = spark.read.gitLocal(pathToRepos)

  }



}
