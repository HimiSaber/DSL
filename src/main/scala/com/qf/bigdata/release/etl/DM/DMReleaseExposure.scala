package com.qf.bigdata.release.etl.DM

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

object DMReleaseExposure {
  val logger: Logger = LoggerFactory.getLogger(DMReleaseExposure.getClass)





  def main(args: Array[String]): Unit = {
    //开始和结束日期
    val begin:String ="20190909"
    val end:String = "20190909"

    //应用名字
    val appName = "dm_exposure"
    //提交应用
    submitJob(appName,begin,end)

  }

  def submitJob(appName: String,begin:String,end:String): Unit ={
    var ss: SparkSession= null

    try{
      //创建sparkconf
      val conf: SparkConf = new SparkConf()
        //hive动态分区
        .set("hive.exec.dynamic.partition","true")
        //分区非严格模式
        .set("hive.exec.dynamic.partition.mode","nonstrict")
        //spark shuffle分区数量，适当调节
        .set("spark.sql.shuffle.partition","8")
        //map器件进行merge，便于传输
        .set("hive.merge.mapfile","true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        //spark自动广播变量阈值
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        //spark允许笛卡尔积
        .set("spark.sql.crossJoin.enabled", "true")
        .setAppName(appName)
        .setMaster("local[*]")
      //通过自定义的SparkHelper创建sparksession
     ss = SparkHelper.createSpark(conf)

      //校验起止时间,获取所有日子
      val date: Seq[String] = SparkHelper.rangeDates(begin,end)

      date.foreach(date=>{
        runJob(ss,date)
      })
    }catch {
      //print error
      case e:Exception =>logger.error(e.getMessage,e)

    }finally {
      //close spark
      if (ss != null) {ss.stop()}
    }

  }





  def runJob(ss:SparkSession,day:String): Unit ={

    //set persist
    val storagelevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL

    //set saveMode
    val savemode: SaveMode = ReleaseConstant.DEF_SAVEMODE

    // 获取当天日志字段数据
    val cusomerColumns = DMReleaseColumnsHelper.selectDMReleaseExposureTmp()
    // get explosure fields
    val explosure: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMReleaseExposure()
    // 当天数据，设置条件，根据条件进行查询，后续调用数据
    val cusomerReleaseCondition =
      (col(s"dre.${ReleaseConstant.DEF_PARTITION}")) === lit(day)

    // 填入条件
    val tmpDF: Dataset[Row] = SparkHelper
      .readTableData(ss,ReleaseConstant.DW_RELEASE_EXPOSURE,"dre",ReleaseConstant.DW_RELEASE_CUSTOMER,"drc",Seq(ReleaseConstant.COL_RELEASE_SESSION),cusomerColumns)
      // 查询条件
      .where(cusomerReleaseCondition)
      .persist(storagelevel)


    println("查询结束======================结果显示")
    import ss.implicits._
    //agg
    import org.apache.spark.sql.functions._



    //渠道曝光统计
    //逻辑处理，分组
    val columns: Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}")

    val explosureDF: DataFrame = tmpDF.groupBy(columns: _*).agg(
      sum("expl").alias("expl"),
      (sum("click") / sum("expl")).alias("rate"),
      max(ReleaseConstant.DEF_PARTITION).alias(ReleaseConstant.DEF_PARTITION)
    ).selectExpr(explosure: _*)
    explosureDF.show(10,false)


    //曝光多维度cube
    val explosureCubeColumn: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDMReleaseExposureCube()
    val cubeColumns: Seq[Column] = Seq[Column]($"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
      $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
      $"${ReleaseConstant.COL_RELEASE_GENDER}",
      $"${ReleaseConstant.COL_RELEASE_AREA_CODE}")

    val cubeRet: DataFrame = tmpDF.cube(cubeColumns: _*).agg(
      sum("expl").alias("expl"),
      (sum("click") / sum("expl")).alias("rate"),
      max(ReleaseConstant.DEF_PARTITION).alias(ReleaseConstant.DEF_PARTITION)
    ).selectExpr(explosureCubeColumn: _*)
    println("多维统计================")
    cubeRet.show(20,false)


  }


}
