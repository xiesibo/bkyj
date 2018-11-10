package com.snp.bd.bkyj.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by x on 2018/6/23.
 */
public class BsQyModel implements Serializable{

    private String name;
    private String typename;
    private String hash;
    private Date date;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setTypename(String typename) {
        this.typename = typename;
    }

    public String getTypename() {
        return typename;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getHash() {
        return hash;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date getDate() {
        return date;
    }

}
