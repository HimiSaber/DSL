package com.qf.bigdata.release.etl.DW

import scala.collection.mutable.ArrayBuffer

/**
  * DW 获取日志字段
  */
object DWReleaseColumnsHelper {

  /**
    * 目标客户
    */
  def selectDWReleaseColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num ")
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("get_json_object(exts,'$.idcard') as idcard ")
    columns.+=("(cast(date_format(now(),'yyyy')as int) - cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{6})(\\\\d{4})',2)as int)) as age ")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{16})(\\\\d{1})',2)as int) % 2 as gender ")
    columns.+=("get_json_object(exts,'$.area_code') as area_code ")
    columns.+=("get_json_object(exts,'$.longitude') as longitude ")
    columns.+=("get_json_object(exts,'$.latitude') as latitude ")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id ")
    columns.+=("get_json_object(exts,'$.model_code') as model_code ")
    columns.+=("get_json_object(exts,'$.model_version') as model_version ")
    columns.+=("get_json_object(exts,'$.aid') as aid ")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光主题
    */
  def selectDWExplodColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num ")
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 注册主题
    */
  def selectDWRegisterColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') as user_id")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num ")
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 点击主题
    */
  def selectDWClickrColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    //columns.+=("get_json_object(exts,'$.user_register') as user_id")
    columns.+=("ods.release_session")
    columns.+=("ods.release_status")
    columns.+=("ods.device_num")
    columns.+=("ods.device_type")
    columns.+=("ods.sources")
    columns.+=("ods.channels")

    columns.+=("get_json_object(tsj.exts,'$.idcard') as idcard")
    columns.+=("(cast(date_format(now(),'yyyy') as int) - cast( regexp_extract(get_json_object(tsj.exts,'$.idcard'), '(\\\\d{6})(\\\\d{4})(\\\\d{4})', 2) as int) )/10000 as age")
    columns.+=("cast(regexp_extract(get_json_object(tsj.exts,'$.idcard'), '(\\\\d{6})(\\\\d{8})(\\\\d{4})', 3) as int) % 2 as gender")
    columns.+=("get_json_object(tsj.exts,'$.area_code') as area_code")
    columns.+=("get_json_object(tsj.exts,'$.longitude') as longitude")

    columns.+=("get_json_object(tsj.exts,'$.latitude') as latitude")
    columns.+=("get_json_object(tsj.exts,'$.matter_id') as matter_id")

    columns.+=("get_json_object(tsj.exts,'$.model_code') as model_code")
    columns.+=("get_json_object(tsj.exts,'$.model_version') as model_version")
    columns.+=("get_json_object(tsj.exts,'$.aid') as aid")

    columns.+=("ods.ct")
    columns.+=("ods.bdp_day")
    columns
  }



}
