package tech.sourced.api

import org.apache.spark.sql.{DataFrameReader, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class RepoDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "git-local"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new RepositoryRelation(sqlContext, parameters("path"))

}
