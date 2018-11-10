package com.snp.bd.bkyj.model;

import java.util.Date;

/**
 * 案件信息模型
 * Created by wxh-pc on 2017/11/19.
 */
public class CaseInfoModel {

    private int id;          //主键
    private String code;     //编码
    private String name;     //案件名称
    private int nature;      //性质1
    private int nature_sub;   //性质2   性质1的子集
    private String remark;     //案件描述
    private String file_ids;    //附件id，多个id以逗号分隔
    private String user_ids;    //案件处理人ID  ， 多个id以逗号分隔
    private int input_id;     //创建人id
    private Date create_time;   //创建时间
    private int status;             //状态     1-草稿  2-审批中   3-正式   4-驳回  5-结案
    private String description;   //申请描述

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNature() {
        return nature;
    }

    public void setNature(int nature) {
        this.nature = nature;
    }

    public int getNature_sub() {
        return nature_sub;
    }

    public void setNature_sub(int nature_sub) {
        this.nature_sub = nature_sub;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getFile_ids() {
        return file_ids;
    }

    public void setFile_ids(String file_ids) {
        this.file_ids = file_ids;
    }

    public String getUser_ids() {
        return user_ids;
    }

    public void setUser_ids(String user_ids) {
        this.user_ids = user_ids;
    }

    public int getInput_id() {
        return input_id;
    }

    public void setInput_id(int input_id) {
        this.input_id = input_id;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
