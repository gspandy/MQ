/**
 *
 */
package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNullable;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class UnregisterClientRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String clientID;

    @CFNullable
    private String producerGroup;
    @CFNullable
    private String consumerGroup;


    public String getClientID() {
        return clientID;
    }


    public void setClientID(String clientID) {
        this.clientID = clientID;
    }


    public String getProducerGroup() {
        return producerGroup;
    }


    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }


    public String getConsumerGroup() {
        return consumerGroup;
    }


    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }


    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }
}
