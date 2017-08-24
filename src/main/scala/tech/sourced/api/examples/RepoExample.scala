package tech.sourced.api.examples

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
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

    reposDF.select("repoURL").show()


    // through API
    import spark.implicits._
    reposDF.filter(reposDF("isFork") === true).show()
    reposDF.filter($"isFork" === false).show()

    // more advanced
    import Update._
    val reposDF2 = spark.read.localGit(pathToRepos)
    reposDF2.select("size").show()
  }

}


object Update {
  implicit class RepoDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def localGit(path: String): DataFrame = reader.format("git-local").load(path)
  }
}