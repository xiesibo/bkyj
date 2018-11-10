package com.snp.bd.bkyj.msg;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.math3.analysis.function.Abs;
import scala.tools.nsc.transform.SpecializeTypes;

/**
 * Created by Administrator on 2017/11/27.
 */
public class AbstractMessage {

    protected String messageType;

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public static AbstractMessage convert(String message) {
        return JSONObject.parseObject(message, AbstractMessage.class);
    }

}
