package com.eb.bi.rs.frame2.algorithm.svd.svd

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.eb.bi.rs.frame2.common.pluginutil.PluginConfig
import com.eb.bi.rs.frame2.common.pluginutil.PluginExitCode
import com.eb.bi.rs.frame2.common.pluginutil.PluginResult
import com.eb.bi.rs.frame2.common.pluginutil.PluginUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
 * Created by liyang on 2016/11/16.
 */
object SVD {
  //var is variable; val is immutable
  var pluginUtil: PluginUtil = null
  var log: Logger = null

  def main(args: Array[String]) {
    pluginUtil = PluginUtil.getInstance
    pluginUtil.init(args)
    log = pluginUtil.getLogger
    Logger.getRootLogger.setLevel(Level.WARN)
    val begin: Date = new Date

    //get the recommendations of every user
    val ret = getRecom()

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

  def getRecom(): Int = {
    val conf = new SparkConf().setAppName("moviesvd")
    val sc = new SparkContext(conf)

    val config: PluginConfig = pluginUtil.getConfig
    val inputPath = config.getParam("ratings_input_path", "/user/ebupt/liyang/spark/svd/data/ratings.dat")
    val moviePath = config.getParam("movies_input_path", "/user/ebupt/liyang/spark/svd/data/movies.dat")
    val rank = config.getParam("rank", 50)
    val numIterations = config.getParam("numIterations", 20)
    val lambda = config.getParam("lambda", 0.01)
    val recomNum = config.getParam("recomNum", 5)
    val outputPath = config.getParam("movie_recom_output_path", "/user/ebupt/liyang/spark/svd/recommendations")

    //get the rating data
    val data = sc.textFile(inputPath)
    val ratings = data.map(_.split("::") match {
      case Array(user, item, score, time) => Rating(user.toInt, item.toInt, score.toDouble)
    }).cache()

    //get the user--movie List
    val movieList = ratings.map {
      case Rating(user, item, score) => (user, item)
    }.distinct()
      .groupByKey()

    //get the movie--name list
    val movieId = sc.textFile(moviePath)
      .map(_.split("::") match {
        case Array(movieId, movieName, movieType) => (movieId.toInt, movieName)
      })

    val model = ALS.train(ratings, rank, numIterations, lambda)

    val movieName = movieId.collectAsMap()
    val moviesFilter = movieList.collectAsMap()

    val result = model.recommendProductsForUsers(recomNum).map {
      case (userId, recommendations) => {
        var recomStr = ""
        val movieFilter = moviesFilter.getOrElse(userId, List()).toList
        for (r <- recommendations) {
          if (!movieFilter.contains(r.product)) {
            recomStr = recomStr + r.product + ":" + movieName.getOrElse(r.product, "") + "," + r.rating + "|"
          }
        }
        if (recomStr.endsWith("|")) {
          recomStr = recomStr.substring(0, recomStr.length - 1)
        }
        (userId, recomStr)
      }
    }
    check(outputPath)
    result.saveAsTextFile(outputPath)
    sc.stop()
    if (result == None) 1 else 0
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
