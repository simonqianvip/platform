package com.kkb.engine

//import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.util.ReentrantGuard
import com.kkb.domain.engine.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author weiqian
 * @date 2020/4/10 8:58 PM
 * @description ${description}
 **/
class EngineSession(platEngine:String,_tag:Option[String]){
//  引擎信息
  def engineInfo:String = platEngine
// 引擎唯一标识
  def tag:Option[String] = _tag

  private val lock = new ReentrantLock()
//  锁通信
  private val condition = lock.newCondition()
//  是否退出线程等待状态
  private val stopped:Boolean = false
  private var throwable:Throwable = null

  /**
   * 保存批处理作业信息
   */
  val batchJob = new ConcurrentHashMap[String,Job]()

  /**
   * 让主线程等待子线程结束后再结束，否则可能出现僵尸进程
   * @param timeout
   * @return
   */
  def awaitTermination(timeout:Long = -1):Boolean ={
//    加锁
    lock.lock()
    try{
//      如果时间小于0，则说明是没给参数，所以直接等待
      if(timeout <0){
        while (!stopped && throwable == null){
          condition.await()
        }
      }else{
        val nanos = TimeUnit.MICROSECONDS.toNanos(timeout)
        while (!stopped && throwable == null && nanos >0){
          condition.awaitNanos(nanos)
        }
      }
      if(throwable != null){
        throw throwable
      }
//      如果线程走到这里，说明当前线程已经被停止或者任务超时
      stopped
    }finally {
      lock.unlock()
    }
  }

}
object EngineSession {
  /**
   * 创建spark引擎
   * @param sparkConf
   * @return
   */
  def createSpark(sparkConf: SparkConf): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("my platform")
      .config(sparkConf)
      //      动态资源调整
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.dynamicAllocation.executorIdleTimeout", "30s")
      .config("spark.dynamicAllocation.maxExecutors", "100")
      .config("spark.dynamicAllocation.minExecutors", "0")
      //      动态分区
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", 20000)
      //      调度模式
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.executor.memoryOverhead", "512")
      //      序列化
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      //      添加hive支持，需要添加hive-site.xml配置文件
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")
    spark
  }

}
