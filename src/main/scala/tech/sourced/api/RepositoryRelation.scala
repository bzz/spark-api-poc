package tech.sourced.api

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._

case class RepositoryRelation(sqlContext: SQLContext, path: String)  extends BaseRelation with PrunedFilteredScan {

  override def schema: StructType = StructType(Seq(
    StructField("repoUrl", StringType, false),
    StructField("isFork", BooleanType, false),
    StructField("UUID", StringType, false),
    StructField("size", LongType, false))
  )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // val repositories = HDFS.listDirs(path)

    //repositories.map { repo =>

    // JGit open "${repo}${File.separator}.git"
    //  val ref
    //  val UUID = ref.getName.split('/').last
    //  val isFork = config.getString("remote", UUID, "isfork")
    //  val repoUrls = config.getStringList("remote", UUID, "url")
    //  val mainUrl = if (repoUrls.isEmpty) "" else repoUrls.head
    //
    //  Row(mainUrl, isFork, UUID)
    //}


    //delegate to CustomGitRDD
    sqlContext.sparkContext.parallelize(Seq(
      Row("git@github.com:src-d/go-git.git", false, "UUID1", 1000),
      Row("https://github.com/orirawlings/go-git.git", true, "UUID2", 1025),
      Row("https://github.com/git/git.git", false, "UUID3", 1023),
      Row("git@github.com:eclipse/jgit.git", false, "UUID4", 2000)
    ))
  }
}
