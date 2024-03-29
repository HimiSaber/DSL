package com.qf.bigdata.release.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * 常量工具类
  */
object ReleaseConstant {

  // partition
  val DEF_STORAGE_LEVEL :StorageLevel = StorageLevel.MEMORY_AND_DISK
  val DEF_SAVEMODE :SaveMode = SaveMode.Overwrite
  val DEF_PARTITION:String = "bdp_day"
  val DEF_PARTITION_WITH_ALIAS:String = "ods.bdp_day"
  val DEF_SOURCE_PARTITIONS = 4

  // 维度列
  val COL_RELEASE_SESSION="release_session"
  val COL_RELEASE_SESSION_STATUS :String = "release_status"
  val COL_RELEASE_SESSION_STATUS_WITH_ALIAS :String = "ods.release_status"
  val COL_RELEASE_SOURCES = "sources"
  val COL_RELEASE_CHANNELS = "channels"
  val COL_RELEASE_DEVICE_TYPE = "device_type"
  val COL_RELEASE_DEVICE_NUM = "device_num"
  val COL_RELEASE_AGE_RANGE = "age_range"
  val COL_RELEASE_GENDER = "gender"
  val COL_RELEASE_AREA_CODE = "area_code"
  val COL_RELEASE_AID = "aid"

  val COL_RELEASE_USER_ID = "user_id"
  val COL_RELEASE_USER_COUNT= "user_count"
  val COL_RELEASE_TOTAL_COUNT= "total_count"



  // ods==============================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"
  val TMP_SESSION_JSON = "ods_release.tmp_session_json"




  //dim==========================
  val DIM_RELEASE_SOURCES_MAPPING_CHANNELS= "dim_release.sources_mapping_channels"
  // dw===============================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
  val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"
  val DW_RELEASE_REGISTER = "dw_release.dw_release_register_users"
  val DW_RELEASE_CLICK= "dw_release.dw_release_click"


  //dm================




}
