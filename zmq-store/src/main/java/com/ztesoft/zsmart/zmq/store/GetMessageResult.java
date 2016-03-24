package com.ztesoft.zsmart.zmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 访问消息返回结果 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月24日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public class GetMessageResult {
    // 多个连续的消息集合
    private final List<SelectMapedBufferResult> messageMapedList = new ArrayList<SelectMapedBufferResult>(100);

    // 用来向Consumer传送消息
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>();

    // 枚举变量，取消息结果
    private GetMessageStatus status;

    // 当被过滤后，返回下一次开始的Offset
    private long nextBeginOffset;

    // 逻辑队列中的最小Offset
    private long minOffset;

    // 逻辑队列中的最大Offset
    private long maxOffset;

    // bytebuffer 总字节数
    private int bufferTotalSize = 0;

    // 是否建议从slave拉消息
    private boolean suggestPullingFromSlave = false;

    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMapedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMapedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
    }

    public void release() {
        for (SelectMapedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }
}
