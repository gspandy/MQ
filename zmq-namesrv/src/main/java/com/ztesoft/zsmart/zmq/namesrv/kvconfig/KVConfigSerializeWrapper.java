package com.ztesoft.zsmart.zmq.namesrv.kvconfig;

import java.util.HashMap;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * KV配置序列化 json包装 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.namesrv.kvconfig <br>
 */
public class KVConfigSerializeWrapper extends RemotingSerializable {
    private HashMap<String /* namespace */, HashMap<String /* key */, String/* value */>> configTable;

    public HashMap<String, HashMap<String, String>> getConfigTable() {
        return configTable;
    }

    public void setConfigTable(HashMap<String, HashMap<String, String>> configTable) {
        this.configTable = configTable;
    }
}
