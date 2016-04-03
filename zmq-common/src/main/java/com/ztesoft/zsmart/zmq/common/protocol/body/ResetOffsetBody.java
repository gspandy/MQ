package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.Map;

import com.ztesoft.zsmart.zmq.common.message.MessageQueue;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;


/**
 * 重置offset 处理结果
 * 
 * @author J.Wang
 *
 */
public class ResetOffsetBody extends RemotingSerializable {
    private Map<MessageQueue, Long> offsetTable;


    public Map<MessageQueue, Long> getOffsetTable() {
        return offsetTable;
    }


    public void setOffsetTable(Map<MessageQueue, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }

}
