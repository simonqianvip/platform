package com.kkb.engine

/**
 * @author weiqian
 * @date 2020/5/11 10:21 PM
 * @description ${description}
 **/

object GlobalGroupId {
  /**
   * 全局唯一ID
   */
  var groupId:Int = 0

//  获取全局唯一ID
  def getGroupId:Int = {
    this.synchronized{
      groupId +=1
      groupId
    }
  }

}
