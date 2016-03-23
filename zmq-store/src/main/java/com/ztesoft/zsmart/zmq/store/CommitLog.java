package com.ztesoft.zsmart.zmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * Store all metadata downtime for recovery, data protection reliability <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月23日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public class CommitLog {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    // Message's MAGIC CODE daa320a7
    public final static int MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8;

    // End of file empty MAGIC CODE cbd43194
    private final static int BlankMagicCode = 0xBBCCDDEE ^ 1880681586 + 8;

}
