package com.snp.bd.bkyj.model;

import org.apache.calcite.util.Static;

import java.io.Serializable;

/**
 * Created by x on 2018/6/20.
 */
public class QuyuModel implements Serializable{
    private String interval;//时间间隔分钟
    private String range;
    private String geohash;
    private String date;
    private String lower_left;
    private String upper_right;
    private String type;
    private Double radius;//半径m
    private String point;
    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Double getRadius() {
        return radius;
    }

    public void setRadius(Double radius) {
        this.radius = radius;
    }

    public String getLower_left() {
        return lower_left;
    }

    public void setLower_left(String lower_left) {
        this.lower_left = lower_left;
    }

    public String getUpper_right() {
        return upper_right;
    }

    public void setUpper_right(String upper_right) {
        this.upper_right = upper_right;
    }

    public String getPoint() {
        return point;
    }

    public void setPoint(String point) {
        this.point = point;
    }
}
