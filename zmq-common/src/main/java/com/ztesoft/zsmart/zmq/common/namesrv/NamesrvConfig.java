package com.ztesoft.zsmart.zmq.common.namesrv;

import java.io.File;

import com.ztesoft.zsmart.zmq.common.MixAll;

/**
 * Name Server 的配置类 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.namesrv <br>
 */
public class NamesrvConfig {
    private String zmqHome = System.getProperty(MixAll.ZMQ_HOME_PROPERTY, System.getenv(MixAll.ZMQ_HOME_ENV));

    // 通用的KV配置持久化地址
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator
        + "kvConfig.json";

    public String getZmqHome() {
        return zmqHome;
    }

    public void setZmqHome(String zmqHome) {
        this.zmqHome = zmqHome;
    }

    public String getKvConfigPath() {
        return kvConfigPath;
    }

    public void setKvConfigPath(String kvConfigPath) {
        this.kvConfigPath = kvConfigPath;
    }
}
