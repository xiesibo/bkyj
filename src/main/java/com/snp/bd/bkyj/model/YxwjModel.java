package com.snp.bd.bkyj.model;

import java.util.Date;

/**
 * Created by x on 2018/5/31.
 */
public class YxwjModel {
    /**
     * 异常类型1（当天拨打电话超过2小时）
     */
    public static final String EX_TH_2="1";
    public static final String EX_TH_0_4_1="2";
    public static final String EX_TH_0_4_TD_1="3";
    public static final String EX_THCS_5="4";
    public static final String EX_TH_10="5";
    public static final String EX_TH_5="6";
    public static final String EX_KD_15="7";
    public static final String EX_KD_30="8";
    public static final String EX_WB_15="9";
    public static final String EX_WB_20="10";
    public static final String EX_TH_3FC="11";
    public static final String EX_TH_0_4_3FC="12";
    //当天凌晨0-4点拨打特定电话的通话时间与过去一个月均值的偏差超过过去一个月(季度)3倍均方差
    public static final String EX_TH_0_4_TD3FC="13";
    //
    public static final String EX_THCS_3FC="14";
    public static final String EX_THCS_0_4_3FC="15";
    public static final String EX_THCS_0_4_TD3FC="16";
    public static final String EX_KD_3FC="17";
    public static final String EX_WB_3FC="18";

    private Long ID;
    //对应wj_key_area表，多个区域id用“,”隔开
    private String QYID;
    //对应sa_userinfo表，多个推送用户id用“,”隔开
    private String YHID;
    //根据异常类型来判断属于什么异常规则
    private String YCLX;
    //异常说明
    private String YCSM;
    //是否生效
    private Integer SFSX;
    //更新时间
    private Date GXSJ;
    //内容模板
    private String NRMB;
    //生效时间
    private Date SXSJ;
    //录入人ID
    private String INPUTERINPUTER;
    //
    private String JSONPARAM;

    public Long getId() {
        return ID;
    }

    public void setID(Long ID) {
        this.ID = ID;
    }

    public String getQYID() {
        return QYID;
    }

    public void setQYID(String QYID) {
        this.QYID = QYID;
    }

    public String getYHID() {
        return YHID;
    }

    public void setYHID(String YHID) {
        this.YHID = YHID;
    }

    public String getYCLX() {
        return YCLX;
    }

    public void setYCLX(String YCLX) {
        this.YCLX = YCLX;
    }

    public String getYCSM() {
        return YCSM;
    }

    public void setYCSM(String YCSM) {
        this.YCSM = YCSM;
    }

    public Integer getSFSX() {
        return SFSX;
    }

    public void setSFSX(Integer SFSX) {
        this.SFSX = SFSX;
    }

    public Date getGXSJ() {
        return GXSJ;
    }

    public void setGXSJ(Date GXSJ) {
        this.GXSJ = GXSJ;
    }

    public String getNRMB() {
        return NRMB;
    }

    public void setNRMB(String NRMB) {
        this.NRMB = NRMB;
    }

    public Date getSXSJ() {
        return SXSJ;
    }

    public void setSXSJ(Date SXSJ) {
        this.SXSJ = SXSJ;
    }

    public String getINPUTERINPUTER() {
        return INPUTERINPUTER;
    }

    public void setINPUTERINPUTER(String INPUTERINPUTER) {
        this.INPUTERINPUTER = INPUTERINPUTER;
    }

    public String getJSONPARAM() {
        return JSONPARAM;
    }

    public void setJSONPARAM(String JSONPARAM) {
        this.JSONPARAM = JSONPARAM;
    }
}
