package com.eb.bi.rs.frame2.algorithm.svd.mse

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.eb.bi.rs.frame2.common.pluginutil.{PluginConfig, PluginExitCode, PluginResult, PluginUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{Level, Logger}

/**
 * Created by liyang on 2016/11/18.
 */
object MSE {

  var pluginUtil: PluginUtil = null
  var log: Logger = null

  def main(args: Array[String]) {

    pluginUtil = PluginUtil.getInstance()
    pluginUtil.init(args)
    log = pluginUtil.getLogger
    Logger.getRootLogger.setLevel(Level.WARN)
    val begin: Date = new Date

    val ret = trainData()

    val end: Date = new Date
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    val endTime: String = format.format(end)
    val timeCost: Long = end.getTime - begin.getTime

    val result: PluginResult = pluginUtil.getResult
    result.setParam("endTime", endTime)
    result.setParam("timeCosts", timeCost)
    result.setParam("exitCode", if (ret == 0) PluginExitCode.PE_SUCC else PluginExitCode.PE_LOGIC_ERR)
    result.setParam("exitDesc", if (ret == 0) "run successfully" else "run failed.")
    result.save

    log.info("time cost in total(s): " + (timeCost / 1000.0))
    System.exit(ret)
  }

  def trainData(): Int ={
    val conf = new SparkConf().setAppName("movieMSE")
    val sc = new SparkContext(conf)

    val config: PluginConfig = pluginUtil.getConfig
    val inputPath = config.getParam("ratings_input_path","/user/ebupt/liyang/spark/svd/data/ratings.dat")
    val rank = config.getParam("rank","10,20,30,40,50")
    val numIterations = config.getParam("numIterations","10,20")
    val lambda = config.getParam("lambda","0.1,0.01,0.0001")
    val outputPath = config.getParam("mse_output_path","/user/ebupt/liyang/spark/svd/result")

    //get the input data
    val data = sc.textFile(inputPath)
    val ratings = data.map(_.split("::") match {
      case Array(user, item, score, time) => Rating(user.toInt, item.toInt, score.toDouble)
    }).cache()

    //get the parameters
    val rankList = rank.split(",").toList
    val numIterationsList = numIterations.split(",").toList
    val lambdaList = lambda.split(",").toList

    val arrBuff = new ArrayBuffer[String]()
    for (iteration <- numIterationsList) {
      for (rank <- rankList) {
        for (lambda <- lambdaList) {
          val mse = getMSE(ratings, rank.toInt, iteration.toInt, lambda.toDouble)
          arrBuff += (s"(rank: $rank, iteration: $iteration, lambda: $lambda, " +
            s"Explicit) Mean Squared Error = " + mse)
        }
      }
    }
    arrBuff.toArray.mkString("\\\t")
    val result = sc.parallelize(arrBuff)
    check(outputPath)
    result.saveAsTextFile(outputPath)
    sc.stop()
    if(result == None) 1 else 0
  }

  def getMSE(ratings: RDD[Rating], iteration: Int, rank: Int, lambda: Double): Double = {

    //build the model
    val model = ALS.train(ratings, iteration, rank, lambda)

    //predict the score
    val userItem = ratings.map {
      case Rating(user, item, score) => (user, item)
    }
    val preResult = model.predict(userItem).map {
      case Rating(user, item, score) => ((user, item), score)
    }

    val MSE = ratings.map {
      case Rating(user, item, score) => ((user, item), score)
    }.join(preResult).map {
      case ((user, item), (r1, r2)) =>
        val error = r2 - r1;
        error * error
    }.mean()

    MSE
  }

  def check(path: String): Unit = {
    println("Begin delete!--" + path)
    val output = new Path(path)
    val hdfs = FileSystem.get(URI.create(path), new Configuration())
    // delete the existed path
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + path)
    }
  }
}

