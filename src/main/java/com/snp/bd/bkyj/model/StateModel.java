package com.snp.bd.bkyj.model;

import java.util.Date;
import java.util.List;

/**
 *
 * 状态模型
 * Created by Administrator on 2017/12/2.
 */
public class StateModel {

     private float lat;  //纬度
    private float lng; //经度
    private String date;  //时间


    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLng() {
        return lng;
    }

    public void setLng(float lng) {
        this.lng = lng;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
