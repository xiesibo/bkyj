package com.snp.bd.bkyj.model;

import java.util.List;

/**
 * Created by x on 2018/6/23.
 */
import java.util.List;
import java.util.Date;

/**
 * Auto-generated: 2018-06-23 21:26:9
 *
 * @author bejson.com (i@bejson.com)
 * @website http://www.bejson.com/java2pojo/
 */
public class BsfxTaskModel {

    private String relation;
    private String time;
    private String id;
    private String imsi;
    private List<BsQyModel> data;
    private Date beginTime;
    private Date endTime;
    private String target;
    private int taskid;
    public void setRelation(String relation) {
        this.relation = relation;
    }
    public String getRelation() {
        return relation;
    }

    public void setTime(String time) {
        this.time = time;
    }
    public String getTime() {
        return time;
    }

    public void setId(String id) {
        this.id = id;
    }
    public String getId() {
        return id;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }
    public String getImsi() {
        return imsi;
    }

    public void setData(List<BsQyModel> data) {
        this.data = data;
    }
    public List<BsQyModel> getData() {
        return data;
    }

    public void setBeginTime(Date beginTime) {
        this.beginTime = beginTime;
    }
    public Date getBeginTime() {
        return beginTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    public Date getEndTime() {
        return endTime;
    }

    public void setTarget(String target) {
        this.target = target;
    }
    public String getTarget() {
        return target;
    }

    public void setTaskid(int taskid) {
        this.taskid = taskid;
    }
    public int getTaskid() {
        return taskid;
    }

}