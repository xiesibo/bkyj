package com.snp.bd.bkyj.model;

import java.io.Serializable;
import java.util.List;

/**
 * Created by x on 2018/6/20.
 */
public class PzfxTaskModel implements Serializable {
   private String relation;
    private List<QuyuModel> data;
    private String taskid;

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public List<QuyuModel> getData() {
        return data;
    }

    public void setData(List<QuyuModel> data) {
        this.data = data;
    }

    public String getTaskid() {
        return taskid;
    }

    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }
}
