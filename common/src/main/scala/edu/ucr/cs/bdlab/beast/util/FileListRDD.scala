package edu.ucr.cs.bdlab.beast.util

import org.apache.hadoop.fs.{FileStatus, FileSystem, GlobFilter, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FileListRDD {
  def listFiles(sc: SparkContext, dir: String, globPattern: String = "*",
                recursive: Boolean = false, numPartitions: Int = 0): RDD[String] = {
    val np = if (numPartitions == 0) sc.defaultParallelism else numPartitions
    val path: Path = new Path(dir)
    val fs: FileSystem = path.getFileSystem(sc.hadoopConfiguration)
    val filter = new GlobFilter(globPattern)
    val matchingFiles: Array[FileStatus] = fs.globStatus(new Path(path, if (recursive) "**" else "*"), filter)
    if (matchingFiles == null)
      sc.emptyRDD[String]
    else
      sc.parallelize(matchingFiles.map(_.getPath.toString), np)
  }
}