package com.ztesoft.zsmart.zmq.common;

import com.ztesoft.zsmart.zmq.common.constant.PermName;

/**
 * TOPIC 配置 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common <br>
 */
public class TopicConfig {
    public static int DefaultReadQueueNums = 16;

    public static int DefaultWriteQueueNums = 16;

    private static final String SEPARATOR = " ";

    private String topicName;

    private int readQueueNums = DefaultReadQueueNums;

    private int writeQueueNums = DefaultWriteQueueNums;

    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    private int topicSysFlag = 0;

    private boolean order = false;

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();

        // 1
        sb.append(this.topicName);
        sb.append(SEPARATOR);

        // 2
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);

        // 3
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);

        // 4
        sb.append(this.perm);
        sb.append(SEPARATOR);

        // 5
        sb.append(this.topicFilterType);

        return sb.toString();
    }

    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs != null && strs.length == 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            return true;
        }

        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean isOrder) {
        this.order = isOrder;
    }

    @Override
    public boolean equals(Object obj) {
        TopicConfig other = (TopicConfig) obj;
        if (other != null) {
            return this.topicName.equals(other.topicName) && this.readQueueNums == other.readQueueNums
                && this.writeQueueNums == other.writeQueueNums && this.perm == other.perm
                && this.topicFilterType == other.topicFilterType && this.topicSysFlag == other.topicSysFlag
                && this.order == other.order;
        }

        return false;
    }

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums + ", writeQueueNums="
            + writeQueueNums + ", perm=" + PermName.perm2String(perm) + ", topicFilterType=" + topicFilterType
            + ", topicSysFlag=" + topicSysFlag + ", order=" + order + "]";
    }
}
