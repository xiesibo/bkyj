package com.snp.bd.bkyj.main

import com.alibaba.fastjson.JSON
import com.snp.bd.bkyj.model.{KeyAreaModel, PzfxTaskModel, QuyuModel}
import com.snp.bd.bkyj.rule.RuleCompareService

/**
  * Created by x on 2018/7/3.
  */
object Test1 {
  def main(args: Array[String]) {
    var quyus: java.util.List[QuyuModel] = null
    val task ="{\"relation\":\"1\",\"data\":[{\"interval\":\"5\",\"range\":98957,\"grohash\":\"wmnhq2uk\",\"date\":\"2016-12-12 20:19:10\",\"lower_left\":\"[113.94092,111.94092]\",\"upper_right\":\"[27.03622,25.03622]\",\"type\":\"Polygon\"},{\"interval\":\"5\",\"range\":\"500\",\"grohash\":\"wmpx29vm\",\"date\":\"2018-06-20 21:12:51\",\"type\":\"Circle\",\"radius\":92658.1306091436,\"point\":\"[111.826399,29.410044]\"}],\"taskid\":22754}"
    val taskobj = JSON.parseObject(task, classOf[PzfxTaskModel]).asInstanceOf[PzfxTaskModel]
    quyus = taskobj.getData
    for (i <- 0 until quyus.size()) {
      println(existErea(quyus.get(i), 26.114f, 113.105f))
    }
  }
  def existErea(quyu: QuyuModel, lat: Float, lng: Float): Boolean = {
    if(quyu.getType.equals("Circle")){
      val point: String = quyu.getPoint
      val r: Float = quyu.getRadius.toFloat
      val index: Int = point.indexOf(',')
      val x1: Float = lat
      val y1: Float = lng
      val r1: Float = (Math.sqrt(Math.pow(x1 - point.substring(1, index).toFloat, 2) + Math.pow(y1 - point.substring(index + 1, point.length - 1).toFloat, 2))).toFloat
      if (r1 <= r) {
        return true
      }
      else {
        return false
      }
    } else {
      val low_left: String = quyu.getLower_left
      val up_right: String = quyu.getUpper_right
      val index1: Int = low_left.indexOf(',')
      val xmin: Float = low_left.substring(1, index1).toFloat
      val ymin: Float = low_left.substring(index1 + 1, low_left.length - 1).toFloat

      val index2: Int = up_right.indexOf(',')
      val xmax: Float = up_right.substring(1, index2).toFloat
      val ymax: Float = up_right.substring(index2 + 1, up_right.length - 1).toFloat

      val r: KeyAreaModel.Rectangle = new KeyAreaModel.Rectangle
      r.setXmin(xmin)
      r.setXmax(xmax)
      r.setYmin(ymin)
      r.setYmax(ymax)
      if (lng >= r.getXmin() && lng <= r.getXmax() && lat >= r.getYmin() && lat <= r.getYmax())
        return true;
      else
        return false;
    }
  }
}
