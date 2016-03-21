package com.ztesoft.zsmart.zmq.remoting.protocol;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;

/**
 * define remoting object serialize abstract class <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.protocol <br>
 */
public abstract class RemotingSerializable {

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }

    /**
     * serialize to json: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param obj
     * @param prettyFormat
     * @return <br>
     */
    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    /**
     * json to obj: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param json
     * @param clazz
     * @return <br>
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(Charset.forName("UTF-8"));
        }
        return null;
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, Charset.forName("UTF-8"));
        return fromJson(json, classOfT);
    }

}
