package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * 删除订阅组请求参数
 * 
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-8-22
 */
public class DeleteSubscriptionGroupRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String groupName;


    @Override
    public void checkFields() throws RemotingCommandException {
    }


    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
