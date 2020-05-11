package com.kkb.utils

import com.alibaba.fastjson.JSONObject
import java.util
import java.util.Properties

/**
 * @author weiqian
 * @date 2020/4/15 10:54 PM
 * @description ${description}
 **/

object DataStruct {
  /**
   * 转json
   * @param tuples
   * @return
   */
  def convertJson(tuples:(String,Any)*):JSONObject={
    tuples.foldLeft(new JSONObject()){
      case (obj,(k,v))=>obj.put(k,v)
        obj
    }
  }

  /**
   * 转map
   * @param tuples
   * @return
   */
  def convertMap(tuples:(String,String)*):java.util.HashMap[String,String]={
    tuples.foldLeft(new util.HashMap[String,String]()){
      case (map,(k,v)) =>map.put(k,v)
        map
    }
  }

  /**
   * 转properties
   * @param tuples
   * @return
   */
  def converProp(tuples:(String,String)*):Properties={
    tuples.foldLeft(new Properties()){
      case(prop,(k,v))=>prop.setProperty(k,v)
        prop
    }
  }

}
