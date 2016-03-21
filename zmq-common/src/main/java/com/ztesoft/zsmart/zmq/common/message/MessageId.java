package com.ztesoft.zsmart.zmq.common.message;

import java.net.SocketAddress;

/**
 * messageId <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.message <br>
 */
public class MessageId {
    private SocketAddress address;

    private long offset;

    public MessageId(SocketAddress address, long offset) {
        this.address = address;
        this.offset = offset;
    }

    public SocketAddress getAddress() {
        return address;
    }

    public void setAddress(SocketAddress address) {
        this.address = address;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
