package tech.sourced.api

import org.apache.spark.sql.DataFrameReader


class RepoDataFrameReader(val reader: DataFrameReader) extends AnyVal {
  def localGit(path: String) = reader.format("git-local").load(path)
}
