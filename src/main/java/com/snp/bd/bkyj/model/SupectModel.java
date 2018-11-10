package com.snp.bd.bkyj.model;

/**
 * 线索信息模型
 * Created by wxh-pc on 2017/11/19.
 */
public class SupectModel {

    public static final int DATA_TYPE_PHONE = 1;
    public static final int DATA_TYPE_WECHART = 2;
    public static final int DATA_TYPE_QQ = 3;
    public static final int DATA_TYPE_MAC = 4;
    public static final int DATA_TYPE_IMEI = 5;
    public static final int DATA_TYPE_IMSI = 6;
    public static final int DATA_TYPE_CARD = 7;

    private Long id;   //主键
    private int type;   //类型  1-案件  2-重点人员
    private int bus_id;    //业务ID    案件ID  或者  重点人员对应的二级性质ID
    private int data_type;   //数据类型     1-phone  2-wechat 3-qq  4-mac  5-imei  6-misi 7-CARD身份证
    private String data_value;

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

    public int getBus_id() {
        return bus_id;
    }

    public void setBus_id(int bus_id) {
        this.bus_id = bus_id;
    }

    public int getData_type() {
        return data_type;
    }

    public void setData_type(int data_type) {
        this.data_type = data_type;
    }

    public String getData_value() {
        return data_value;
    }

    public void setData_value(String data_value) {
        this.data_value = data_value;
    }

    @Override
    public String toString() {
        return "SupectModel{" +
                "id=" + id +
                ", type=" + type +
                ", bus_id=" + bus_id +
                ", data_type=" + data_type +
                ", data_value='" + data_value + '\'' +
                '}';
    }
}
