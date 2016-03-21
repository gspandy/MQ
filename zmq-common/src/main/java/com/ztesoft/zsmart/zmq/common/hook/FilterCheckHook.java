package com.ztesoft.zsmart.zmq.common.hook;

import java.nio.ByteBuffer;

/**
 * 确认消息是否需要过滤 Hook
 * <Description> <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.hook <br>
 */
public interface FilterCheckHook {
    public String hookName();

    public boolean isFilterMatched(final boolean isUnitMode, final ByteBuffer byteBuffer);

}
