/**
 * $Id: DeleteTopicRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 14-09-15
 */
public class CloneGroupOffsetRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String srcGroup;
    @CFNotNull
    private String destGroup;
    private String topic;
    private boolean offline;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getDestGroup() {
        return destGroup;
    }


    public void setDestGroup(String destGroup) {
        this.destGroup = destGroup;
    }


    public String getTopic() {
        return topic;
    }


    public void setTopic(String topic) {
        this.topic = topic;
    }


    public String getSrcGroup() {

        return srcGroup;
    }


    public void setSrcGroup(String srcGroup) {
        this.srcGroup = srcGroup;
    }


    public boolean isOffline() {
        return offline;
    }


    public void setOffline(boolean offline) {
        this.offline = offline;
    }
}
