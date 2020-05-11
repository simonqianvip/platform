package com.kkb.utils

import com.typesafe.config.ConfigFactory

/**
 * @author weiqian
 * @date 2020/4/15 11:16 PM
 * @description 读取配置文件工具类
 **/

class GlobalConfigUitls {

  /**
   * 加载application.conf配置文件
   * @return
   */
  private def conf = ConfigFactory.load()

  def heartColumnFamily = "MM"

  val getProp = (argv:String) => conf.getString(argv)

}

/**
 * 为了配置访问的安全性
 */
object GlobalConfigUitls extends GlobalConfigUitls
