package com.ztesoft.zsmart.zmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;


public class HeartbeatData extends RemotingSerializable {
    private String clientID;

    private Set<ProducerData> producerDataSet = new HashSet<ProducerData>();

    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();


    public String getClientID() {
        return clientID;
    }


    public void setClientID(String clientID) {
        this.clientID = clientID;
    }


    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }


    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }


    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }


    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }


    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
                + ", consumerDataSet=" + consumerDataSet + "]";
    }
}
