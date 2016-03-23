package com.ztesoft.zsmart.zmq.store;

import java.nio.ByteBuffer;

/**
 * Write messages callback interface <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月23日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public interface AppendMessageCallback {
    /**
     * After message serialization, write MapedByteBuffer
     * 
     * @param byteBuffer
     * @param maxBlank
     * @param msg
     * @return How many bytes to write
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
        final Object msg);
}
