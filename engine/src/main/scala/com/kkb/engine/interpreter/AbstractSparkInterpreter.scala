package com.kkb.engine.interpreter

import java.io.ByteArrayOutputStream

import com.kkb.engine.EngineSession
import org.apache.spark.SparkConf
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.JsonDSL._

import scala.tools.nsc.interpreter.Results

/**
 * @author weiqian
 * @date 2020/4/10 8:48 PM
 * @description 对解释器类增强
 **/

object AbstractSparkInterpreter {
  //  正则表达式，匹配换行符
  private[interpreter] val KEEP_NEWLINE_REGEX = """(?<=\n)""".r

}

//对接口进行功能增强
abstract class AbstractSparkInterpreter extends Interpreter {

  /*
  引入伴生类内容
   */

  import AbstractSparkInterpreter._

  //  操作流，构建输出流，用于spark—shell的回显
  protected val outputStream = new ByteArrayOutputStream()

  //  给底层实现，判断是否启动sparkShell
  protected def isStarted(): Boolean

  //  是否中断spark-shell
  protected def interpreter(order: String): Results.Result

  //  spark shell绑定变量
  protected def bind(
                      name: String,
                      className: String,
                      value: Object,
                      modifier: List[String]
                    )

  //  关闭spark shell
  override def close() = {}

  /**
   * 读取流中的数据
   */
  private def readStdout(): String = {
    val output: String = outputStream.toString("UTF-8")
    outputStream.reset()
    output
  }

  /**
   * 解析错误信息
   */
  protected[interpreter] def parseError(stdout: String): (String, Seq[String]) = {
    val lines = KEEP_NEWLINE_REGEX.split(stdout)
    val traceback = lines.tail
    val errValues = lines.headOption.map(_.trim).getOrElse("UNKNOW ERROR")
    (errValues, traceback)
  }

  private def executeLine(order: String): Interpreter.ExecuteResponse = {
    scala.Console.withOut(outputStream) {
      interpreter(order) match {
        case Results.Success => Interpreter.ExecuteSuccess(TEXT_PLAIN -> readStdout())
        case Results.Error => {
          val tuple: (String, Seq[String]) = parseError(readStdout())
          Interpreter.ExecuteError("ERROR", tuple._1, tuple._2)
        }
        case Results.Incomplete => Interpreter.ExecuteIncomplete()
      }
    }
  }

  protected def restoreContextClassLoader[T](fn: => T): T = {
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    try {
      fn
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader)
    }
  }


  private def executeLines(lines: List[String], resultFromLastLine: Interpreter.ExecuteResponse): Interpreter.ExecuteResponse = {
    lines match {
      case Nil => resultFromLastLine
      case head :: tail =>
        val result = executeLine(head)

        result match {
          case Interpreter.ExecuteSuccess(data) =>

            val response = resultFromLastLine match {

              case Interpreter.ExecuteSuccess(ds) =>
                //                合并输入的代码内容text_plain -> 内容合并
                if (data.values.contains(TEXT_PLAIN) && ds.values.contains(TEXT_PLAIN)) {
                  val lastRet = data.values.getOrElse(TEXT_PLAIN, "").asInstanceOf[String]
                  val currentRet = ds.values.getOrElse(TEXT_PLAIN, "").asInstanceOf[String]

                  if (lastRet.nonEmpty && currentRet.nonEmpty) {
                    Interpreter.ExecuteSuccess(TEXT_PLAIN -> s"${lastRet}${currentRet}")
                  } else if (lastRet.nonEmpty) {
                    Interpreter.ExecuteSuccess(TEXT_PLAIN -> lastRet)
                  } else if (currentRet.nonEmpty) {
                    Interpreter.ExecuteSuccess(TEXT_PLAIN -> currentRet)
                  } else {
                    result
                  }
                } else {
                  result
                }

              case Interpreter.ExecuteError(_, _, _) => result

              case Interpreter.ExecuteAborted(_) => result

              case Interpreter.ExecuteIncomplete() =>
                tail match {
                  case Nil => {
                    executeLine(s"{\n$head\n}") match {
                      case Interpreter.ExecuteIncomplete() | Interpreter.ExecuteError(_, _, _) => result
                      case _ => resultFromLastLine
                    }
                  }
                  case next :: nextTail =>
                    executeLines(head + "\n" + next :: nextTail, resultFromLastLine)
                }

              case _ => result
            }

            executeLines(tail, response)
        }

    }
  }

  /**
   * 解析代码
   *
   * @param order
   * @return
   */
  override def execute(order: String): Interpreter.ExecuteResponse = {
    //    打破双亲委派模型，程序会有限走executeLines这个方法
    restoreContextClassLoader {
      require(isStarted())
      executeLines(order.trim.split("\n").toList, Interpreter.ExecuteSuccess(
        JObject(
          (TEXT_PLAIN, JString(""))
        )
      ))
    }
  }

  def createSparkConf(conf: SparkConf): SparkConf = {
    val spark = EngineSession.createSpark(conf)
    bind("spark", spark.getClass.getCanonicalName, spark, List("""@transient"""))
    execute("import org.apache.spark.SparkContext._ \n import spark.implicits._")
    execute("import spark.sql")
    execute("import org.apache.spark.sql.functions._")
    spark.sparkContext.getConf
  }


}
