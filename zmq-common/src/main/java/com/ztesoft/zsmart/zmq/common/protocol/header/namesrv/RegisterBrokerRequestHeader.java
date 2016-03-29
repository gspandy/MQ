/**
 * $Id: RegisterBrokerRequestHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.ztesoft.zsmart.zmq.common.protocol.header.namesrv;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * 
 * <Description> <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月29日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.header.namesrv <br>
 */
public class RegisterBrokerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private String haServerAddr;
    @CFNotNull
    private Long brokerId;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public String getBrokerAddr() {
        return brokerAddr;
    }


    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public String getClusterName() {
        return clusterName;
    }


    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }


    public String getHaServerAddr() {
        return haServerAddr;
    }


    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }


    public Long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }
}
