package example.scala.actor

import java.net.InetAddress

import akka.actor.ActorSystem
import akka.pattern.Patterns
import akka.util.Timeout
import com.kkb.domain.CommandMode
import com.kkb.domain.engine.Instruction
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.Duration

/**
 * @author weiqian
 * @date 2020/5/11 11:06 PM
 * @description ${description}
 **/

object Client extends App {
  override def main(args: Array[String]): Unit = {
    val host = InetAddress.getLocalHost.getHostAddress
    val port = 3001
    val conf = ConfigFactory.parseString(
      s"""
        |akka.actor.provider="akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname=${host}
        |akka.remote.netty.tcp.port=${port}
        |""".stripMargin)
    val clientSystem = ActorSystem("client", conf)

    val instruction =
      "val textFile = spark.sparkContext.textFile(\"hdfs://node01:9000/test.dd\");"+
    "val counts = textFile.flatMap(line => line.split(\" \")).map(word =>(word,1)).reduceByKey(_ + _);"+
    "counts.repartition(1).saveAsTextFile(\"hdfs://node01:9000/test.dd6 \");"

    val commandMode = CommandMode.CODE
    val variables = "[]"
    val _token = ""

    val ip = InetAddress.getLocalHost.getHostAddress
    val actorSystemAddr = s"${ip}:3000"
    val actorName = "actor_1"
    val selection = clientSystem.actorSelection("akka.tcp://system@"+actorSystemAddr +"/user/" +actorName)
//    发送akka消息
    Patterns.ask(selection,Instruction(commandMode,instruction,variables,_token),new Timeout(Duration.create(10,"s")))




  }
}
