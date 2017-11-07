package com.eb.bi.rs.newcorrelation

import org.apache.spark.sql.SparkSession

/**
  * Created by lenovo on 2017/7/10.
  */
object readParquet {
  def main (args : Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("yarn")
      .appName("readParquet")
      .getOrCreate()

    delete("hdfs://ebcloud1", "/user/recsys/newcorrelationrec/offline/output/model/trees")
    val test1 = spark.read.parquet("/user/recsys/linwanying/correlationrec/myGradientBoostingRegressionModel/data")
    //, test1("predict").getField("predict"), test1("isLeaf"),
    // test1("split").getField("feature"), test1("split").getField("threshold"), test1("leftNodeId"), test1("rightNodeId")
    test1.filter("treeId == 0").show(10)
    val copys = test1.select(test1("treeId"), test1("nodeId"), test1("predict").getField("predict"), test1("isLeaf"),
      test1("split").getField("feature"), test1("split").getField("threshold"), test1("leftNodeId"), test1("rightNodeId")).rdd
    copys.map{ r =>
      val treeId = r.getInt(0)
      val nodeId = r.getInt(1)
      val predict = r.getDouble(2)
      val isLeaf = r.getBoolean(3)
      val feature =  if (r.isNullAt(4)) "" else r.getInt(4)
      val threshold =  if (r.isNullAt(5)) "" else r.getDouble(5)
      val leftNode =  if (r.isNullAt(6)) "" else r.getInt(6)
      val rightNode =  if (r.isNullAt(7)) "" else r.getInt(7)
      treeId + "_" + nodeId + ";" + predict + ";" + isLeaf + ";" + feature + ";" + threshold + ";" + leftNode + ";" + rightNode
    }.saveAsTextFile("/user/recsys/newcorrelationrec/offline/output/model/trees")
  }

  def delete(master:String, path:String): Unit = {
    println("Begin delete!--" + master + path)
    val output = new org.apache.hadoop.fs.Path(master + path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(master), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + master + path)
    }
  }

}
