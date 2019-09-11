package com.qf.bigdata.release.etl.DM

import scala.collection.mutable.ArrayBuffer

/**
  * 字段
  */
object DMReleaseColumnsHelper {
  /**
    * 目标客户集市
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("device_num")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("bdp_day")
    columns

  }
  /**
  *  渠道用户统计
    */
  def selectDMReleaseCustomerColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns

  }

  def selectDMReleaseCustomerCubeColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns

  }

  def selectDMReleaseExposureTmp():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("dre.sources")
    columns.+=("dre.channels")
    columns.+=("dre.device_type")
    columns.+=("getAgeRange(age) age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("case dre.release_status  when '03' then 1 else 0 end expl")
    columns.+=("case dre.release_status when '04' then 1 else 0 end click")
    columns.+=("dre.bdp_day")
    columns
  }
  //
  def selectDMReleaseExposure():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("expl")
    columns.+=("rate")
    columns.+=("bdp_day")
    columns
  }
  //曝光多维度cube
  def selectDMReleaseExposureCube():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("expl")
    columns.+=("rate")
    columns
  }


  //注册集市
  def selectDMReleaseRegisterTmp():ArrayBuffer[String]={

    val columns = new ArrayBuffer[String]()
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("user_id")
    columns.+=("bdp_day")
    columns
  }
  def selectDMReleaseClick():ArrayBuffer[String]={

    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("getAgeRange(age) age_range")
    columns.+=("aid")
    columns.+=("bdp_day")
    columns
  }







}
