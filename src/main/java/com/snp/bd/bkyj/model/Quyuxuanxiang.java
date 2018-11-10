package com.snp.bd.bkyj.model;

/**
 * Created by x on 2018/6/24.
 */
public class Quyuxuanxiang {
    private String selectcase;
    private String lower_left;
    private String upper_right;
    private String type;
    private Double radius;//半径m
    private String point;

    public String getSelectcase() {
        return selectcase;
    }

    public void setSelectcase(String selectcase) {
        this.selectcase = selectcase;
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
