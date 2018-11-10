package com.snp.bd.bkyj.msg;

import com.alibaba.fastjson.JSONObject;

/**
 *
 * 话单消息（含有经纬度信息）
 *
 *
 * Created by Administrator on 2017/11/23.
 */
public class HDMessage extends AbstractMessage{

    private String usernum; //手机号
    private String homearea; //归属地
    private String imsi;
    private String imei;
    private String begintime;  //开始时间
    private String latitude;   //纬度
    private String longitude;  //经度


    public static HDMessage c(String message){
        return JSONObject.parseObject(message,HDMessage.class);
    }


    public String getUsernum() {
        return usernum;
    }

    public void setUsernum(String usernum) {
        this.usernum = usernum;
    }

    public String getHomearea() {
        return homearea;
    }

    public void setHomearea(String homearea) {
        this.homearea = homearea;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getBegintime() {
        return begintime;
    }

    public void setBegintime(String begintime) {
        this.begintime = begintime;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
}
