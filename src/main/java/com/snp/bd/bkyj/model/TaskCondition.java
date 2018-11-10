package com.snp.bd.bkyj.model;

import java.sql.Date;

/**
 * Created by x on 2018/8/3.
 */
public class TaskCondition {
    private int ID;
    private String CONDITION;
    private String TASKNAME;
    private int TYPE;
    private int STATUS;
    private String ABNORMAL;
    private int INPUTER;
    private Date INPUTDATE;
    private int CASEID;
    private String ISREAD;

    public TaskCondition() {
    }

    public TaskCondition(int ID, String CONDITION, String TASKNAME, int TYPE, int STATUS, String ABNORMAL, int INPUTER, Date INPUTDATE, int CASEID, String ISREAD) {
        this.ID = ID;
        this.CONDITION = CONDITION;
        this.TASKNAME = TASKNAME;
        this.TYPE = TYPE;
        this.STATUS = STATUS;
        this.ABNORMAL = ABNORMAL;
        this.INPUTER = INPUTER;
        this.INPUTDATE = INPUTDATE;
        this.CASEID = CASEID;
        this.ISREAD = ISREAD;
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getCONDITION() {
        return CONDITION;
    }

    public void setCONDITION(String CONDITION) {
        this.CONDITION = CONDITION;
    }

    public String getTASKNAME() {
        return TASKNAME;
    }

    public void setTASKNAME(String TASKNAME) {
        this.TASKNAME = TASKNAME;
    }

    public int getTYPE() {
        return TYPE;
    }

    public void setTYPE(int TYPE) {
        this.TYPE = TYPE;
    }

    public int getSTATUS() {
        return STATUS;
    }

    public void setSTATUS(int STATUS) {
        this.STATUS = STATUS;
    }

    public String getABNORMAL() {
        return ABNORMAL;
    }

    public void setABNORMAL(String ABNORMAL) {
        this.ABNORMAL = ABNORMAL;
    }

    public int getINPUTER() {
        return INPUTER;
    }

    public void setINPUTER(int INPUTER) {
        this.INPUTER = INPUTER;
    }

    public Date getINPUTDATE() {
        return INPUTDATE;
    }

    public void setINPUTDATE(Date INPUTDATE) {
        this.INPUTDATE = INPUTDATE;
    }

    public int getCASEID() {
        return CASEID;
    }

    public void setCASEID(int CASEID) {
        this.CASEID = CASEID;
    }

    public String getISREAD() {
        return ISREAD;
    }

    public void setISREAD(String ISREAD) {
        this.ISREAD = ISREAD;
    }
}
