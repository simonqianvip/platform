package com.kkb.domain

import java.sql.Timestamp

import com.kkb.utils.DataStruct

/**
 * @author weiqian
 * @date 2020/4/15 10:32 PM
 * @description ${description}
 **/

object engine {

  case class Job(
                instruction:String ="",//指令
                var mode:String = CommandMode.SQL,//默认指令，推荐走sql模式
                startTime:Timestamp=new Timestamp(System.currentTimeMillis()),//记录任务的开始时间
                var takeTime:Long = 0,//执行花费的时间
                var user:String = "",//执行用户
                var success:Boolean = true,
                var hdfs_path:String = "",
                var dataType:String = ResultDataType.STRUCTURED,
                var data:String = "",
                var variables:String = StructType.Struct,
                var schema:String = "",//结果的schema信息
                var jobStatus: String = JobStatus.RUNNING,
                var engineInfoAndGroup:String = ""
                ){
    /**
     * 将job任务结构化数据转json
     * @return
     */
    def struct:Any ={
      DataStruct.convertJson(
        Seq(
          ("instruction",instruction),
          ("mode",mode),
          ("startTime",startTime),
            ("takeTime",takeTime),
          ("user",user),
          ("success",success),
          ("hdfs_path",hdfs_path),
          ("dataType",dataType),
          ("data",data),
          ("variables",variables),
          ("schema",schema),
          ("jobStatus",jobStatus),
          ("engineInfoAndGroup",engineInfoAndGroup)
        ):_*
      )
    }



  }
  //引擎： engineInfo:ip:port
  case class PlatEngine(engineId:Int,engineInfo:String)
  //指令描述：做用户鉴权
  case class Instruction(commandMode:String,instruction: String,variables:String,token:String)
  //停止引擎
  case class StopEngine()
  //获取批引擎和引擎组的执行结果
  case class FetchBatchResult(engineInfoAndGroup:String)
  //通过引擎信息获取jobId
  case class GetJobIdsForGroup(engineInfoAndGroup:String)
  //获取正在运行的流任务信息
  case class FetchActiveStream()
  //停止流任务
  case class StopStreamJob(streamJobName:String)
  //获取流任务的状态
  case class StreamJobStatus(streamJobName:String)
  //取消任务
  case class CancelJob(groupId:Int)
  //hive的元数据信息
  case class HiveCatalog()
  //自动获取sparkSQL与hive的交互方式
  case class HiveCatalogWithAutoComplete()
  //通过zk获取hbase的表
  case class HbaseTablesForZk(zk:String)
  //获取hive的所有表
  case class HiveTables()

}



/**
 * 指令模式：SQL and code
 */
object CommandMode{
  val SQL = "sql"
  val CODE = "code"
}

/**
 * 结果集类型
 */
object ResultDataType{
  val ERROR = "error"
  val PRE = "pre"
  val STRUCTURED = "structured"
}

/**
 * 结构化数据的边
 */
object StructType{
  val Struct = "[]"
}

/**
 * job的状态
 */
object JobStatus{
  val RUNNING = "RUNNING"
  val FINISH = "FINISH"
  val ERROR = "ERROR"
}
