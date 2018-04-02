package com.xiaoxiang.flink;

/**
 * Created by yuhailin on 2018/3/1.
 */
public class Record {

    private String   bizName;     // 业务名字

    private int     bizId;        // 业务方id

    private long    timestamp;    // 日志产生时间

    private int      attr;        // 属性1

    private String   data;        // 原始属性描述

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public String getBizName() {
        return bizName;
    }

    public void setBizId(int bizId) {
        this.bizId = bizId;
    }

    public int getBizId() {
        return bizId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setAttr(int attr) {
        this.attr = attr;
    }

    public int getAttr() {
        return attr;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}
