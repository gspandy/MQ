package com.ztesoft.zsmart.zmq.common.protocol.header;

import java.util.List;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class GetConsumerListByGroupResponseBody extends RemotingSerializable {
    private List<String> consumerIdList;


    public List<String> getConsumerIdList() {
        return consumerIdList;
    }


    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }
}
