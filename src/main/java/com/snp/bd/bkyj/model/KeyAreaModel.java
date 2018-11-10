package com.snp.bd.bkyj.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Date;

/**
 * 重点区域模型
 * Created by wxh-pc on 2017/11/19.
 */
public class KeyAreaModel {

    private int id; //主键
    private String name; //区域名称
    private String area;  //参考百度 json串
    private Date log_time; //创建时间
    private int log_user; //创建用户
    private int type;  //未知!!!!

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public Date getLog_time() {
        return log_time;
    }

    public void setLog_time(Date log_time) {
        this.log_time = log_time;
    }

    public int getLog_user() {
        return log_user;
    }

    public void setLog_user(int log_user) {
        this.log_user = log_user;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "KeyAreaModel{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", area='" + area + '\'' +
                ", log_time=" + log_time +
                ", log_user=" + log_user +
                ", type=" + type +
                '}';
    }

    public Object convertArea(){

        if(area.contains("circle")){  //圆形
            Circular c = new Circular();
            try {
                JSONObject jo = JSONObject.parseObject(area);
                String point = jo.getString("point");
                float r = jo.getFloat("radius");
                int index = point.indexOf(',');

                c.setX(Float.parseFloat(point.substring(1,index)));
                c.setY(Float.parseFloat(point.substring(index+1,point.length()-1)));
                c.setR(r);
                return c;
            }catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }else if(area.contains("rectangle")){
            JSONObject jo = JSONObject.parseObject(area);
            String low_left = jo.getString("lower_left");
            String up_right = jo.getString("upper_right");
            int index1 = low_left.indexOf(',');
            float xmin = Float.parseFloat(low_left.substring(1,index1));
            float ymin = Float.parseFloat(low_left.substring(index1+1,low_left.length()-1));

            int index2 = up_right.indexOf(',');
            float xmax = Float.parseFloat(up_right.substring(1,index2));
            float ymax = Float.parseFloat(up_right.substring(index2+1,up_right.length()-1));

            Rectangle r = new Rectangle();
            r.setXmin(xmin);
            r.setXmax(xmax);
            r.setYmin(ymin);
            r.setYmax(ymax);
            return r;
        }else{
            return null;
        }

    }


    public static class Circular{
        private float x;  //经度
        private float y;  //纬度
        private float r;

        public float getX() {
            return x;
        }

        public void setX(float x) {
            this.x = x;
        }

        public float getY() {
            return y;
        }

        public void setY(float y) {
            this.y = y;
        }

        public float getR() {
            return r;
        }

        public void setR(float r) {
            this.r = r;
        }
    }

    public static class Rectangle{

        private float xmin;
        private float xmax;
        private float ymin;
        private float ymax;

        public float getXmin() {
            return xmin;
        }

        public void setXmin(float xmin) {
            this.xmin = xmin;
        }

        public float getXmax() {
            return xmax;
        }

        public void setXmax(float xmax) {
            this.xmax = xmax;
        }

        public float getYmin() {
            return ymin;
        }

        public void setYmin(float ymin) {
            this.ymin = ymin;
        }

        public float getYmax() {
            return ymax;
        }

        public void setYmax(float ymax) {
            this.ymax = ymax;
        }
    }


}
