/**
  * Copyright 2018 bejson.com 
  */
package com.snp.bd.bkyj.model;
import java.util.Date;
import java.util.List;

/**
 * Auto-generated: 2018-06-24 10:43:30
 *
 * @author bejson.com (i@bejson.com)
 * @website http://www.bejson.com/java2pojo/
 */
public class DtjcxTaskModel {

    private String caseInfo;
    private Date beginDate;
    private Date endDate;
    private List<Quyuxuanxiang> quyuxuanxiang;
    private List<Xingweitiaojian> xingweitiaojian;
    private List<Qitatiaojian> qitatiaojian;
    private int taskid;
    public void setCaseInfo(String caseInfo) {
         this.caseInfo = caseInfo;
     }
     public String getCaseInfo() {
         return caseInfo;
     }

    public void setBeginDate(Date beginDate) {
         this.beginDate = beginDate;
     }
     public Date getBeginDate() {
         return beginDate;
     }

    public void setEndDate(Date endDate) {
         this.endDate = endDate;
     }
     public Date getEndDate() {
         return endDate;
     }

    public void setQuyuxuanxiang(List<Quyuxuanxiang> quyuxuanxiang) {
         this.quyuxuanxiang = quyuxuanxiang;
     }
     public List<Quyuxuanxiang> getQuyuxuanxiang() {
         return quyuxuanxiang;
     }

    public void setXingweitiaojian(List<Xingweitiaojian> xingweitiaojian) {
         this.xingweitiaojian = xingweitiaojian;
     }
     public List<Xingweitiaojian> getXingweitiaojian() {
         return xingweitiaojian;
     }

    public void setQitatiaojian(List<Qitatiaojian> qitatiaojian) {
         this.qitatiaojian = qitatiaojian;
     }
     public List<Qitatiaojian> getQitatiaojian() {
         return qitatiaojian;
     }

    public int getTaskid() {
        return taskid;
    }

    public void setTaskid(int taskid) {
        this.taskid = taskid;
    }
}