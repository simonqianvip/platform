package com.kkb.engine

import java.io.{ByteArrayOutputStream, PrintStream}

import akka.actor.{Actor, Props}
import com.kkb.domain.{CommandMode, ResultDataType}
import com.kkb.domain.engine.{Instruction, Job}
import com.kkb.engine.interpreter.Interpreter.{ExecuteAborted, ExecuteError, ExecuteResponse, ExecuteSuccess}
import com.kkb.engine.interpreter.SparkInterpreter
import com.kkb.logging.Logging
import com.kkb.utils.{GlobalConfigUitls, ZKUtils}
import javafx.print.PrinterJob.JobStatus
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author weiqian
 * @date 2020/4/15 9:55 PM
 * @description ${description}
 **/

class JobActor(
                _interpreter: SparkInterpreter, //解释器
                engineSession: EngineSession, //引擎
                sparkConf: SparkConf //sparkConf
              ) extends Actor with Logging {

  //  拼接引擎路径
  val valid_engine_path = s"${ZKUtils.valid_engine_path}/${engineSession.engineInfo}_${context.self.path.name}"
  var zkClient: ZkClient = _

  var sparkSession: SparkSession = _
  var interpreter: SparkInterpreter = _

  var token: String = _
  var job: Job = _

  //  初始化
  override def preStart(): Unit = {
    warn(
      """
        |
        | █████╗  ██████╗████████╗ ██████╗ ██████╗     ███████╗████████╗ █████╗ ██████╗ ████████╗
        |██╔══██╗██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗    ██╔════╝╚══██╔══╝██╔══██╗██╔══██╗╚══██╔══╝
        |███████║██║        ██║   ██║   ██║██████╔╝    ███████╗   ██║   ███████║██████╔╝   ██║
        |██╔══██║██║        ██║   ██║   ██║██╔══██╗    ╚════██║   ██║   ██╔══██║██╔══██╗   ██║
        |██║  ██║╚██████╗   ██║   ╚██████╔╝██║  ██║    ███████║   ██║   ██║  ██║██║  ██║   ██║
        |╚═╝  ╚═╝ ╚═════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝    ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝
        |
        |
        |""".stripMargin)

    zkClient = ZKUtils.getzkClient(GlobalConfigUitls.getProp("zk.servers"))
    interpreter = _interpreter
    sparkSession = EngineSession.createSpark(sparkConf).newSession()

    ZKUtils.registerActorInPlatEngine(zkClient, valid_engine_path, engineSession.tag.getOrElse("default"))
  }

  //  处理业务
  override def receive: Receive = {
    case Instruction(commandMode, instruction, variables, _taken) => {
      //      所有指令操作，都通过钩子执行
      actorHook() { () =>
        // TODO: 组装命令
        var assemble_instruction = instruction

        token = _taken
        //        封装job，给token和job封装赋值
        job = Job(instruction = instruction, variables = variables)
        //        获取任务组ID
        val groupId = GlobalGroupId.getGroupId
        job.engineInfoAndGroup = s"${engineSession.engineInfo}_${groupId}"
        //        如果有前后端交互，那么把当前任务的唯一标识返回给前段
        sender() ! job

        /*
          提前清除可能存在的线程副本
         */
        sparkSession.sparkContext.clearJobGroup()
        /*
          设置当前作业的描述
         */
        sparkSession.sparkContext.setJobDescription(s"running job:${instruction}")
        /*
          设置线程组ID，后面的作业，由这个线程来启动
         */
        sparkSession.sparkContext.setJobGroup(s"groupId:${groupId}",s"instruction:${instruction}")

        commandMode match {
          case CommandMode.SQL =>

          case CommandMode.CODE =>{
//            打印输入的命令
            info("\n" +("*" * 80) + "\n" + assemble_instruction +"\n" + ("*" * 80))
            job.mode = CommandMode.CODE
//            替换字符串
            assemble_instruction = assemble_instruction.replaceAll("'","\"").replaceAll("\n"," ")
//            解析输入代码，并执行
            val response = interpreter.execute(assemble_instruction)
            while (!storageJobStatus(response)){
              warn("任务未完成")
            }
          }

          case _ =>
        }
      }
    }

  }

  //  结束
  override def postStop(): Unit = {
    warn(
      """
        |
        | █████   ██████ ████████  ██████  ██████      ███████ ████████  █████  ██████  ████████ 
        |██   ██ ██         ██    ██    ██ ██   ██     ██         ██    ██   ██ ██   ██    ██    
        |███████ ██         ██    ██    ██ ██████      ███████    ██    ███████ ██████     ██ 
        |██   ██ ██         ██    ██    ██ ██   ██          ██    ██    ██   ██ ██   ██    ██ 
        |██   ██  ██████    ██     ██████  ██   ██     ███████    ██    ██   ██ ██   ██    ██ 
        |                                                                                     
        |
        |
        |""".stripMargin)
    interpreter.close()
    sparkSession.stop()
  }

  //  actor 的钩子函数
  def actorHook()(func: () => Unit): Unit = {
    try {
      //      规范化，所有业务都走func
      func()
    } catch {
      case e: Exception =>
        //        收集异常信息
        val out = new ByteArrayOutputStream()
        e.printStackTrace(new PrintStream(out))
        val job = Job(data = out.toString(), dataType = ResultDataType.ERROR)
        //        如果出现异常，就把异常回显给客户端
        sender() ! job
    }
  }

  def storageJobStatus(response:ExecuteResponse):Boolean ={
    response match {
      case e:ExecuteSuccess =>{
        val take = (System.currentTimeMillis() -job.startTime.getTime)/1000
        job.takeTime = take
        job.data = e.content.values.values.mkString("\n")
        job.dataType = ResultDataType.PRE
//        job.jobStatus = JobStatus.FINISH
        engineSession.batchJob.put(job.engineInfoAndGroup,job)
        true
      }
//        执行失败的任务
      case e:ExecuteError =>{
        //        job.jobStatus = JobStatus.FINISH
        job.data = e.executeValue
        job.success = false
        job.dataType = ResultDataType.ERROR
        engineSession.batchJob.put(job.engineInfoAndGroup,job)
        true
      }
//        被终止的任务
      case e:ExecuteAborted =>{
        //        job.jobStatus = JobStatus.FINISH
        job.data = e.message
        job.success = false
        job.dataType = ResultDataType.ERROR
        engineSession.batchJob.put(job.engineInfoAndGroup,job)
        true
      }
    }
  }
}

object JobActor {
  //  对外提供伴生方法
  def apply(sparkInterpreter: SparkInterpreter,
            engineSession: EngineSession,
            sparkConf: SparkConf): Props = {
    Props(new JobActor(sparkInterpreter, engineSession, sparkConf))
  }

}
