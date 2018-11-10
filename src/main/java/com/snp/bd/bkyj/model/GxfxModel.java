/**
 * Copyright 2018 bejson.com
 */
package com.snp.bd.bkyj.model;

import java.io.Serializable;

/**
 * Auto-generated: 2018-06-28 9:52:33
 *
 * @author bejson.com (i@bejson.com)
 * @website http://www.bejson.com/java2pojo/
 */
public class GxfxModel implements Serializable{

    private String beginDate;
    private String endDate;
    private int relation;
    private String query;
    private String relationPerson;
private String caseInfo;
    public String getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public int getRelation() {
        return relation;
    }

    public void setRelation(int relation) {
        this.relation = relation;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getRelationPerson() {
        return relationPerson;
    }

    public void setRelationPerson(String relationPerson) {
        this.relationPerson = relationPerson;
    }

    public String getCaseInfo() {
        return caseInfo;
    }

    public void setCaseInfo(String caseInfo) {
        this.caseInfo = caseInfo;
    }
}