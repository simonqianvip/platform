package com.kkb.engine.interpreter

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter.{JPrintWriter, Results}

/**
 * @author weiqian
 * @date 2020/4/10 9:15 PM
 * @description ${description}
 **/

class SparkInterpreter extends AbstractSparkInterpreter {
  private var sparkConf: SparkConf = _
  private var sparkILoop: SparkILoop = _

  //  如果不为空，说明spark-shell已经启动了
  override protected def isStarted(): Boolean = {
    sparkILoop != null
  }

  //  中断spark-shell
  override protected def interpreter(order: String): Result = {
    sparkILoop.interpret(order)
  }

  //  对spark-shell绑定变量
  override protected def bind(name: String, className: String, value: Object, modifier: List[String]): Unit = {
    sparkILoop.beQuietDuring {
      sparkILoop.bind(name, className, value, modifier)
    }
  }

  //关闭
  override def close(): Unit = {
    super.close()
    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  //  注册spark的repl交互，并返回sparkConf
  override def start(): SparkConf = {
    //    判断spark-shell 是否为空
    require(sparkILoop == null)
    val conf = new SparkConf()
    //指定classdir配置，会使用该路径作为class的输出路径，java.io.tmpdir系统临时目录
    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    //    注册使用spark的repl（spark shell的实现是建立在scala repl的基础上的）
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)
    //    指定spark的二进制包
    val execUri = System.getenv("SPARK_EXECUTOR_RUL")
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }

    //    获取类加载器
    val cl = ClassLoader.getSystemClassLoader
    //TODO 动态字节码生成-> 依赖asm->加载jdk环境->通过asm动态生成字节码对象
    val jars = (getUserJars(conf) ++ cl.asInstanceOf[URLClassLoader].getURLs.map(_.toString)).mkString(File.pathSeparator)
    val settings = new Settings()

    /**
     * spark 2.1.0的repl基于scala-2.11的scala.tools.nsc编译工具实现，代码已经相当简洁
     * spark给interp设置了2个关键的配置：-Yrepl-class-based 和 -Yrepl-outdir
     * 通过这2个配置，我们在shell中输入的代码会被编译为class文件输出到执行的文件夹中。
     * 如果指定了spark.repl.classdir配置，会用这个配置的路径作为class文件的输出路径
     */
    settings.processArguments(
      List(
        "-Yrepl-class-based",
        "-Yrepl-outdir",
        s"${outputDir.getAbsolutePath}",
        "-classpath",
        jars
      ), true
    )

    settings.usejavacp.value = true

    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader)

    sparkILoop = new SparkILoop(None, new JPrintWriter(outputStream, true))
    sparkILoop.settings = settings
    sparkILoop.createInterpreter()
    sparkILoop.initializeSynchronous()

    restoreContextClassLoader {
      sparkILoop.setContextClassLoader()
      sparkConf = createSparkConf(conf)
    }

    sparkConf
  }

  /**
   * 根据sparkConf的参数，获取用户的jar包
   *
   * @param conf
   * @param isShell
   * @return
   */
  def getUserJars(conf: SparkConf, isShell: Boolean = false): Seq[String] = {
    val sparkJars = conf.getOption("spark.jars")
    //如果是spark on yarn则合并spark的jar和yarn的jar路径
    if (isShell && conf.get("spark.master") == "yarn") {
      val yarnJars = conf.getOption("spark.yarn.dist.jars")
      //      将spark的jar和yarn的jar路径合并
      unionFileLists(sparkJars, yarnJars).toSeq
    } else {
      sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
    }
  }

  /**
   * jar包的路径合并
   * jdk目录下的jar和spark目录下的jar的路径做合并
   *
   * @param leftList
   * @param rightList
   * @return
   */
  def unionFileLists(leftList: Option[String], rightList: Option[String]): Seq[String] = {
    var allFiles = Seq[String]()
    leftList.foreach(value => allFiles ++= value.split(","))
    rightList.foreach(value => allFiles ++= value.split(","))
    allFiles.filter(_.nonEmpty)
  }
}
