package com.snp.bd.bkyj.model;

import com.alibaba.fastjson.JSONObject;

import java.util.Date;

/**
 * 预警结果， 需要存储到关系型数据库表
 * Created by wxh-pc on 2017/11/19.
 */
public class WarningModel {

    public static final int SOURCE_TYPE_JD = 1; //寄递
    public static final int SOURCE_TYPE_HD = 2; //话单
    public static final int SOURCE_TYPE_DX = 3; //短信
    public static final int SOURCE_WIFI_CW = 4; //wifi场外
    public static final int SOURCE_WIFI_CN = 5; //场内
    public static final int SOURCE_TYPE_WB = 6; //网吧
    public static final int SOURCE_TYPE_WX  = 7; //微信
    public static final int SOURCE_TYPE_DW = 8; //电围

    private String id;
    private int buinesstype;  //1-案件
    private int businessid;   //业务ID
    private Date happentime; //发生时间
    private int sourcetype;  //数据来源  @ref ConstrantModel
    private Long ruleid; //告警规则ID
    private String msgcontent;  //消息内容
    private int touser; //消息接收者
    private Date totime;  //消息发送时间
    private int isready ;    //1-已读  2-未读
    private int isAttention; //布控预警1，隐性挖掘2
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getBuinesstype() {
        return buinesstype;
    }

    public void setBuinesstype(int buinesstype) {
        this.buinesstype = buinesstype;
    }

    public int getBusinessid() {
        return businessid;
    }

    public void setBusinessid(int businessid) {
        this.businessid = businessid;
    }

    public Date getHappentime() {
        return happentime;
    }

    public void setHappentime(Date happentime) {
        this.happentime = happentime;
    }

    public int getSourcetype() {
        return sourcetype;
    }

    public void setSourcetype(int sourcetype) {
        this.sourcetype = sourcetype;
    }

    public Long getRuleid() {
        return ruleid;
    }

    public void setRuleid(Long ruleid) {
        this.ruleid = ruleid;
    }

    public String getMsgcontent() {
        return msgcontent;
    }

    public void setMsgcontent(String msgcontent) {
        this.msgcontent = msgcontent;
    }

    public int getTouser() {
        return touser;
    }

    public void setTouser(int touser) {
        this.touser = touser;
    }

    public Date getTotime() {
        return totime;
    }

    public void setTotime(Date totime) {
        this.totime = totime;
    }

    public int getIsready() {
        return isready;
    }

    public void setIsready(int isready) {
        this.isready = isready;
    }

    public int getIsAttention() {
        return isAttention;
    }

    public void setIsAttention(int isAttention) {
        this.isAttention = isAttention;
    }

    public String toJSON(){
       return JSONObject.toJSONString(this);}
}
