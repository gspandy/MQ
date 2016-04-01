package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.concurrent.ConcurrentHashMap;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * Consumer 消费进度 序列化包装<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年4月1日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.body <br>
 */
public class ConsumerOffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentHashMap<String /* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable = new ConcurrentHashMap<String /*
                                                                                                                                      * topic
                                                                                                                                      * @
                                                                                                                                      * group
                                                                                                                                      */, ConcurrentHashMap<Integer, Long>>();

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

}
