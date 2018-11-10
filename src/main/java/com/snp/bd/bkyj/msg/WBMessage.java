package com.snp.bd.bkyj.msg;

import java.util.Date;

/**
 * Created by Administrator on 2017/11/23.
 */
public class WBMessage extends AbstractMessage{

    private String id_card;
    private Date up_time;
    private Date down_time;
    private String net_code;
    private float lat;
    private float lng;

    public String getId_card() {
        return id_card;
    }

    public void setId_card(String id_card) {
        this.id_card = id_card;
    }

    public Date getUp_time() {
        return up_time;
    }

    public void setUp_time(Date up_time) {
        this.up_time = up_time;
    }

    public Date getDown_time() {
        return down_time;
    }

    public void setDown_time(Date down_time) {
        this.down_time = down_time;
    }

    public String getNet_code() {
        return net_code;
    }

    public void setNet_code(String net_code) {
        this.net_code = net_code;
    }

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
}
