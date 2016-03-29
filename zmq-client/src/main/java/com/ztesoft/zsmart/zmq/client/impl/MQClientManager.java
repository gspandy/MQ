package com.ztesoft.zsmart.zmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ztesoft.zsmart.zmq.client.ClientConfig;
import com.ztesoft.zsmart.zmq.client.impl.factory.MQClientInstance;
import com.ztesoft.zsmart.zmq.remoting.RPCHook;

public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger(0);

    private ConcurrentHashMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }
    
    public static MQClientManager getInstance() {
        return instance;
    }
    
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig,RPCHook rpcHook){
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if(instance == null){
            instance = 
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
            }  
        }
        
        return instance;
    }
    
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }


    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
