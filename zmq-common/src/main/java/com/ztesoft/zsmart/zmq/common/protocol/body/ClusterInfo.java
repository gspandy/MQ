package com.ztesoft.zsmart.zmq.common.protocol.body;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.ztesoft.zsmart.zmq.common.protocol.route.BrokerData;
import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * 协议中传输对象，内容为集群信息 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.body <br>
 */
public class ClusterInfo extends RemotingSerializable {
    public HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

    private HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    public HashMap<String, BrokerData> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    public void setBrokerAddrTable(HashMap<String, BrokerData> brokerAddrTable) {
        this.brokerAddrTable = brokerAddrTable;
    }

    public HashMap<String, Set<String>> getClusterAddrTable() {
        return clusterAddrTable;
    }

    public void setClusterAddrTable(HashMap<String, Set<String>> clusterAddrTable) {
        this.clusterAddrTable = clusterAddrTable;
    }

    /**
     * 检索所有addr根据集群: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param cluster
     * @return <br>
     */
    public String[] retrieveAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<String>();
        if (clusterAddrTable.containsKey(cluster)) {
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }

        return addrs.toArray(new String[] {});
    }

    /**
     * 
     * 检索所有集群信息: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @return <br>
     */
    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[] {});
    }
}
