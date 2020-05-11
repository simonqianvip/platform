package com.kkb.engine.interpreter

import org.apache.spark.SparkConf
import org.json4s.JsonAST.JObject


/**
 * @author weiqian
 * @date 2020/4/9 12:51 PM
 * @description 解析引擎
 **/

object Interpreter {

  //抽象类：把引擎的所有的相应统一管理
  abstract class ExecuteResponse

  //  执行成功的任务
  case class ExecuteSuccess(content:JObject) extends ExecuteResponse

  //  执行失败的任务
  case class ExecuteError(
                           executeName: String, //任务名称
                           executeValue: String, //失败原因
                           traceback: Seq[String]//失败的堆栈信息
                         ) extends ExecuteResponse

  //  未完成的任务
  case class ExecuteIncomplete() extends ExecuteResponse

  //  终止任务
  case class ExecuteAborted(message: String) extends ExecuteResponse

}

/**
 * 对外提供方法
 */
trait Interpreter {

  import Interpreter._

  /**
   * 启动方法
   * @return
   */
  def start(): SparkConf

  /**
   * 关闭引擎
   */
  def close(): Unit

  /**
   * 命令执行方法，为保证安全，包可见
   */
  private[interpreter] def execute(order: String): ExecuteResponse
}

