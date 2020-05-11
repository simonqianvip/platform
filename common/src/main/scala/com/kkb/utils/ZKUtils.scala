package com.kkb.utils

import com.kkb.domain.engine.PlatEngine
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.zookeeper.data.Stat

import scala.collection.immutable.Nil

/**
 * @author weiqian
 * @date 2020/4/15 11:18 PM
 * @description zk工具类
 **/

object ZKUtils {
  var zkClient: ZkClient = null
//  session超时时间
  val sessionTimeOut = 60000
//  连接超时时间
  val connectionTimeOut = 60000
//  引擎路径
  var engine_path = "/platform/engine"
//  actor的引擎路径
  var valid_engine_path = "/platform/valid_engine"

  /**
   * 获取服务器地址获取zkClient
   * @param zkServer
   * @return
   */
  def getzkClient(zkServer: String): ZkClient = {
    if (zkClient == null) {
      zkClient = new ZkClient(zkServer, sessionTimeOut, connectionTimeOut, new ZkSerializer {
        //序列化，把里面的内容按照utf8处理
        override def serialize(o: Any): Array[Byte] = {
          try {
            o.toString.getBytes("UTF-8")
          } catch {
            case e: ZkMarshallingError => return null//如果有错误，随便处理一下
          }
        }
        //处理反序列化
        override def deserialize(bytes: Array[Byte]): AnyRef = {
          try {
            new String(bytes, "UTF-8")
          } catch {
            case e: ZkMarshallingError => return null
          }
        }
      })
//      最后，保证程序退出后，zk的客户端也要退出
      sys.addShutdownHook{
        zkClient.close()
      }
    }
    zkClient
  }

  /**
   * 创建父目录
   *
   * @param zkClient
   * @param path
   */
  def createPersistentPathIfNotExists(zkClient: ZkClient, path: String): Unit = {
    if (!zkClient.exists(path)) {
      //      父目录不存在优先创建父目录，再创建子目录
      zkClient.createPersistent(path, true)
    }
  }

  /**
   * 创建父目录
   *
   * @param zkClient
   * @param path
   */
  def createParentPath(zkClient: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf("/"))
    if (parentDir.length != 0) {
      zkClient.createPersistent(parentDir, true)
    }
  }

  def createEphemeralPathAndParentPathIfNotExists(zkClient: ZkClient, path: String, data: String): Unit = {
    zkClient.createEphemeral(path, data)
  }

  /**
   * 向zk中注册actor信息
   *
   * @param zkClient
   * @param id
   * @param host
   * @param port
   */
  def registerEngineInZk(zkClient: ZkClient, id: Int, host: String, port: Int) = {
    val brokerIdPath = ZKUtils.engine_path + s"${id}"
    val brokerInfo = s"${host}:${port}"
    try {
      createPersistentPathIfNotExists(zkClient, ZKUtils.engine_path)
      createEphemeralPathAndParentPathIfNotExists(zkClient, brokerIdPath, brokerInfo)
    } catch {
      case e: ZkNodeExistsException => {
        throw new RuntimeException("注册失败，节点已存在")
      }
    }
  }

  /**
   * 获取platEngine信息
   *
   * @param zkClient
   * @param engineId
   * @return
   */
  def getPlatEngine(zkClient: ZkClient, engineId: Int): Option[PlatEngine] = {
    val dataAndStat: (Option[String], Stat) = ZKUtils.readDataMaybeNotExist(zkClient, ZKUtils.engine_path + s"/${engineId}")
    dataAndStat._1 match {
      case Some(engineInfo) => {
        Some(PlatEngine(engineId, engineInfo))
      }
      case None => None
    }
  }

  /**
   * 读取路径下的数据
   *
   * @param zkClient
   * @param path
   * @return
   */
  def readDataMaybeNotExist(zkClient: ZkClient, path: String): (Option[String], Stat) = {
    //    构建一个stat,里面封装了zk节点信息
    val stat = new Stat()
    //    读取信息，返回（ip:port,节点信息）
    val dataAndStat = try {
      (Some(zkClient.readData(path, stat)), stat)
    } catch {
      case e1: ZkNoNodeException => (None, stat)
      case e2: Throwable => throw e2
    }
    dataAndStat
  }

  /**
   * 获取子节点
   *
   * @param client
   * @param path
   * @return
   */
  def getChildrenMayNotExist(client: ZkClient, path: String): Seq[String] = {
    import scala.collection.JavaConverters._
    try {
      client.getChildren(path).asScala.toSeq
    } catch {
      case e: ZkNoNodeException => return Nil
      case e2: Throwable => throw e2
    }
  }

  /**
   * 从集群多台集群获取PlatEngine
   *
   * @param zkClient
   * @return
   */
  def getPlanEngineInCluster(zkClient: ZkClient): Seq[PlatEngine] = {
    //    得到子节点，子节点用ID表示
    val csd: Seq[String] = ZKUtils.getChildrenMayNotExist(zkClient, engine_path).sorted
    //    返回的是String，类型转换一下
    val csd2i = csd.map(line => line.toInt)
    //    根据节点名称，获取节点里面的信息，最后封装成引擎信息
    val engineDatas = csd2i.map(line => ZKUtils.getPlatEngine(zkClient, line))
    //    得到具体的引擎集合
    val platEngines = engineDatas.filter(_.isDefined).map(line => line.get)
    platEngines
  }

  /**
   * 注册jobActor引擎
   *
   * @param zkClient
   * @param path
   * @param data
   */
  def registerActorInPlatEngine(zkClient: ZkClient, path: String, data: String): Unit = {
    try {
      //      如果父路径不存在，那么先创建父路径
      createPersistentPathIfNotExists(zkClient, ZKUtils.valid_engine_path)
      createEphemeralPathAndParentPathIfNotExists(zkClient, path, data)
    } catch {
      case e: ZkNodeExistsException => {
        throw new RuntimeException("注册jobActor引擎出现异常")
      }
    }
  }


}
