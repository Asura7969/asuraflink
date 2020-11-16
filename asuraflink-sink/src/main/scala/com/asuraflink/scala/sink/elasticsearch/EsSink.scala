package com.asuraflink.scala.sink.elasticsearch

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.{PropertiesSettings, Settings}
import org.elasticsearch.hadoop.rest.{InitializationUtils, RestService}
import org.elasticsearch.hadoop.serialization.builder.ValueWriter
import org.elasticsearch.hadoop.serialization.field.FieldExtractor
import org.elasticsearch.hadoop.serialization.{BytesConverter, JdkBytesConverter}
import org.elasticsearch.spark.serialization.{ScalaMapFieldExtractor, ScalaValueWriter}

import scala.reflect.ClassTag

/**
  *
  * @param indexPrefix es索引前缀
  * @param esType es索引type
  */
class EsSink[T: ClassTag](val indexPrefix:String,
                          val esType:String,
                          val esOptions: Map[String, String],
                          val runtimeMetadata: Boolean = false) extends RichSinkFunction[T]{

  @transient protected lazy val log:Log = LogFactory.getLog(this.getClass)
  lazy val settings:Settings = {
    val settings = new PropertiesSettings().load(CommonUtils.save(esOptions))

    InitializationUtils.setValueWriterIfNotSet(settings, valueWriter, log)
    InitializationUtils.setBytesConverterIfNeeded(settings, bytesConverter, log)
    InitializationUtils.setFieldExtractorIfNotSet(settings, fieldExtractor, log)

    settings
  }

  private var writer: RestService.PartitionWriter = _
  private var dateString = getCurrentYearAndMon()
  protected def valueWriter: Class[_ <: ValueWriter[_]] = classOf[ScalaValueWriter]
  protected def bytesConverter: Class[_ <: BytesConverter] = classOf[JdkBytesConverter]
  protected def fieldExtractor: Class[_ <: FieldExtractor] = classOf[ScalaMapFieldExtractor]


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dateString = getCurrentYearAndMon()
//    getRuntimeContext.getIndexOfThisSubtask
//    getRuntimeContext.getNumberOfParallelSubtasks
    settings.setProperty(ES_RESOURCE_WRITE,s"$indexPrefix$dateString/$esType")
    writer = RestService.createWriter(settings, getRuntimeContext.getIndexOfThisSubtask, -1, log)

  }

  override def invoke(value: T): Unit = {
    val currentDateString = getCurrentYearAndMon()
    if(!dateString.equals(currentDateString)){
      closeWrite()
      settings.setProperty(ES_RESOURCE_WRITE,s"$indexPrefix$currentDateString/$esType")
      writer = RestService.createWriter(settings, getRuntimeContext.getIndexOfThisSubtask, -1, log)
      dateString = currentDateString
    }
    writer.repository.writeToIndex(value)
  }

  override def close(): Unit = {
    super.close()
    writer.close()
  }

  def closeWrite(): Unit ={
    if(null != writer){
      try {
        writer.close()
      }catch {
        case e:Exception =>
          log.error("write to es failed",e)
      } finally {
        writer = null
      }
    }
  }

  def getCurrentYearAndMon(): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(new Date())
  }
}
