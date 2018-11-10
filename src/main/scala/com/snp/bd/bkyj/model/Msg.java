package com.snp.bd.bkyj.model;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by x on 2018/5/20.
 */
public class Msg implements Serializable{
    /**
     * 表名	code	数据编号
     寄递信息
     WJ_MD	1
     事件表
     wj_dw_evt	2
     话单表
     wj_dw_cdr	3
     短信表
     wj_dw_sms	4
     机身码表
     wj_dw_imsi_imei	5
     基站表
     wj_dw_site	6
     CDR数据表
     T_CDR	7
     失踪检测	　szjc	8
     终端特征信息
     wj_wifi_source_fj_1001	9
     被采集热点信息
     wj_wifi_source_fj_1002	10
     终端特征移动采集设备轨迹信息
     wj_wifi_basic_fj_1001	11
     终端特征采集设备基础信息
     wj_wifi_basic_fj_1002	12
     终端上下线信息
     wj_wifi_source_fj_0001	13
     上网日志信息
     wj_wifi_source_fj_0002	14
     网吧信息表
     wj_net	15
     开卡记录表
     wj_net_regist	16
     上下机记录表
     wj_net_use	17
     上网轨迹记录表
     wj_net_trace	18
     上网聊天记录表
     wj_net_chat	19
     朋友圈
     wj_wechat_moment	20
     基站信息
     WJ_WJZ_SITE	21
     轨迹信息
     WJ_WJZ_TRACE	22
     */
    public static int WJ_MD=1;
    public static int wj_dw_evt=2;
    public static int wj_dw_cdr=3;
    public static int wj_dw_sms=4;
    public static int wj_dw_imsi_imei=5;
    public static int T_CDR=7;
    public static int szjc=8;
    public static int wj_wifi_source_fj_1001=9;
    public static int wj_wifi_source_fj_1002=10;
    public static int wj_wifi_basic_fj_1001=11;
    public static int wj_wifi_basic_fj_1002=12;
    public static int wj_wifi_source_fj_0001=13;
    public static int wj_wifi_source_fj_0002=14;
    public static int wj_net_regist=16;
    public static int wj_net_use=17;
    public static int wj_net_trace=18;
    public static int wj_net_chat=19;
    public static int WJ_WJZ_TRACE=22;
    int type;
    Map<String,Object> context;


    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Map<String,Object> getContext() {
        return context;
    }

    public void setContext(Map<String,Object> context) {
        this.context = context;
    }
    public static Msg convert(String message) {
        return JSONObject.parseObject(message,Msg.class);
    }
}
