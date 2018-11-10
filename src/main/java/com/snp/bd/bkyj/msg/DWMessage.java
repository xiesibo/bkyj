package com.snp.bd.bkyj.msg;

import java.util.Date;

/**
 *
 * 电围消息
 *
 *   含有经纬度信息
 *
 * 预警方式：
 *     (1)进入预警
 *     (2)离开预警
 *     (3)聚集预警
 *
 * Created by Administrator on 2017/11/23.
 */
public class DWMessage {

    private String imsi;
    private String imei;
    private Date log_time;
    private float longitude;
    private float latitude;

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

    public Date getLog_time() {
        return log_time;
    }

    public void setLog_time(Date log_time) {
        this.log_time = log_time;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }
}
