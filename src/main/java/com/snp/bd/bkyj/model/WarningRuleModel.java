package com.snp.bd.bkyj.model;

import java.util.Date;

/**
 *
 * 预警规则信息模型
 * Created by Administrator on 2017/11/19.
 */
public class WarningRuleModel {

    public static final int TYPE_ENTER = 1;
    public static final int TYPE_LEAVE = 2;
    public static final int TYPE_JOIN = 3;
    public static final int TYPE_EX = 4;
    public static final int TYPE_MISS = 5;
    public static final int TYPE_DENSITY = 6;
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
    /**
     * 主键ID
     */
    private Long id;
    /**
     * 类型
     * 1-进入  2-离开  3-聚集   4-异常   5-失联  6-密度
     */
    private int type;
    /**
     * 规则类型    1-指定人员    2-比例
     */
    private int rule_type;
    /**
     * 当 规则类型是2的时候 ，多个值以逗号分隔
     */
    private String rule_vlaue;
    /**
     * 人员ID  对应线索表中的ID，多个值以逗号分隔
     */
    private String supecters;
    /**
     * 重点区域 表ID ，多个值以逗号分隔
     */
    private String areas;
    /**
     * 预警方式   1-电话  2-系统通知
     */
    private String warning;
    /**
     * 状态  1-生效  2-失效
     */
    private int status;
    /**
     * 创建人
     */
    private int creater;
    /**
     * 创建时间
     */
    private Date create_time;
    /**
     * 业务类型
     */
    private int bus_type;
    /**
     * 业务ID
     */
    private int bus_id;

    /**
     * 规则名称
     */
    private String rule_name;

    private String phone_number;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getRule_type() {
        return rule_type;
    }

    public void setRule_type(int rule_type) {
        this.rule_type = rule_type;
    }

    public String getRule_vlaue() {
        return rule_vlaue;
    }

    public void setRule_vlaue(String rule_vlaue) {
        this.rule_vlaue = rule_vlaue;
    }

    public String getSupecters() {
        return supecters;
    }

    public void setSupecters(String supecters) {
        this.supecters = supecters;
    }

    public String getAreas() {
        return areas;
    }

    public void setAreas(String areas) {
        this.areas = areas;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getCreater() {
        return creater;
    }

    public void setCreater(int creater) {
        this.creater = creater;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public int getBus_type() {
        return bus_type;
    }

    public void setBus_type(int bus_type) {
        this.bus_type = bus_type;
    }

    public int getBus_id() {
        return bus_id;
    }

    public void setBus_id(int bus_id) {
        this.bus_id = bus_id;
    }

    public String getRule_name() {
        return rule_name;
    }

    public void setRule_name(String rule_name) {
        this.rule_name = rule_name;
    }

    public String getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(String phone_number) {
        this.phone_number = phone_number;
    }

    @Override
    public String toString() {
        return "WarningRuleModel{" +
                "id=" + id +
                ", type=" + type +
                ", rule_type=" + rule_type +
                ", rule_vlaue='" + rule_vlaue + '\'' +
                ", supecters='" + supecters + '\'' +
                ", areas='" + areas + '\'' +
                ", warning='" + warning + '\'' +
                ", status=" + status +
                ", creater=" + creater +
                ", create_time=" + create_time +
                ", bus_type=" + bus_type +
                ", bus_id=" + bus_id +
                '}';
    }
}
