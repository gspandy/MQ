/**
 *
 */
package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class UnregisterClientResponseHeader implements CommandCustomHeader {

    /*
     * (non-Javadoc)
     * 
     * @see com.alibaba.rocketmq.remoting.CommandCustomHeader#checkFields()
     */
    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }

}
