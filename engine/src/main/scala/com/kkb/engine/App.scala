package com.kkb.engine

import akka.actor.ActorSystem
import com.kkb.common.AkkaUtils
import com.kkb.engine.interpreter.SparkInterpreter
import com.kkb.utils.{GlobalConfigUitls, ZKUtils}

/**
 * @author weiqian
 * @date 2020/4/9 12:12 PM
 * @description ${description}
 **/

object App {


  /**
   * 解析参数
   *
   * @param args
   * @return
   */
  def parseArgs(args: Array[String]): Map[String, String] = {
    var argv = args.toList

    var argsMap: Map[String, String] = Map()

    while (argv.nonEmpty) {
      //      这里case的是一个list
      argv match {
        case "-engine.zkServers" :: value :: tail => {
          argsMap += ("zkServers" -> value)
          argv = tail
        }
        case "-engine.tag" :: value :: tail => {
          argsMap += ("engine.tag" -> value)
          argv = tail
        }
        case Nil =>
        case tail => System.err.println(s"对不起无法识别：${tail.mkString(" ")}")
      }
      Thread.sleep(1000)

    }
    argsMap
  }

  def main(args: Array[String]): Unit = {
    //    测试模块1 "-engine.zkServers"
    //    val argv = parseArgs(List("-engine.zkServers","node01:2181","-engine.tag","123").toArray)
    //    println(argv)

    //    测试模块2  -engine.zkServers
    val argv = parseArgs(args)
    val interpreter = new SparkInterpreter
    //  开启spark shell交互
    val sparkConf = interpreter.start()
    sparkConf.set("spark.driver.host", "192.168.80.200")
    //  获取zk信息
    val zkServer = argv.getOrElse("zkServers", GlobalConfigUitls.getProp("zk.servers"))
    val client = ZKUtils.getzkClient(zkServer)

    println(s"zk信息：${zkServer}")
    println(s"zkClient对象：${client}")

    val actorConf = AkkaUtils.getConfig(client)
    val actorSystem = ActorSystem("System", actorConf)

    //    获取akka的信息
    val hostname = actorConf.getString("akka.remote.netty.tcp.hostname")
    val port = actorConf.getString("akka.remote.netty.tcp.port")

    println(s"hostname = ${hostname}")
    println(s"port = ${port}")

//    实例话引擎
    val engineSession = new EngineSession(s"${hostname}:${port}", argv.get("engine.tag"))

//    拿到任务设置并行度
    val paralism = sparkConf.getInt(config.PARALLELISM.key, config.PARALLELISM.defaultValue.get)

//    创建akka通讯模型
    (1 to paralism).foreach(id=>{
      actorSystem.actorOf(JobActor.apply(interpreter,engineSession,sparkConf),name = s"actor_${id}")
    })



  }

}

