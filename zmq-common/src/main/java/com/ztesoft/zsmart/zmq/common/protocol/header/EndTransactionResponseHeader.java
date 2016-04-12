/**
 * $Id: EndTransactionResponseHeader.java 1835 2013-05-16 02:00:50Z shijia.wxr $
 */
package com.ztesoft.zsmart.zmq.common.protocol.header;

import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class EndTransactionResponseHeader implements CommandCustomHeader {

    @Override
    public void checkFields() throws RemotingCommandException {
        // TODO Auto-generated method stub

    }

}
