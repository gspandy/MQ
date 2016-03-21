package com.ztesoft.zsmart.zmq.common.protocol.body;

import com.ztesoft.zsmart.zmq.remoting.protocol.LanguageCode;

/**
 * socket连接data<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.protocol.body <br>
 */
public class Connection {
    private String clientId;

    private String clientAddr;

    private LanguageCode language;

    private int version;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
