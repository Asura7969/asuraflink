package com.asuraflink.scala.sink.elasticsearch

import java.util.Properties

import org.apache.commons.logging.LogFactory
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.hadoop.rest.InitializationUtils
import org.elasticsearch.hadoop.util.IOUtils

object CommonUtils {
  @transient protected lazy val log = LogFactory.getLog(this.getClass)

  def save(conf:Map[String,String]): String ={
    val copy = new Properties()
    conf.foreach(kv => copy.setProperty(kv._1,kv._2))

    val config = new PropertiesSettings().load(IOUtils.propsToString(copy))
    InitializationUtils.discoverEsVersion(config, log)
    InitializationUtils.checkIdForOperation(config)
    InitializationUtils.checkIndexExistence(config)
    config.save()
  }
}
