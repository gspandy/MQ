package com.ztesoft.zsmart.zmq.broker.transaction;

import java.util.List;

/**
 * 事务存储接口 主要为分布式事务消息存储服务 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月31日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.broker.transaction <br>
 */
public interface TransactionStore {

    public boolean open();

    public void close();

    public boolean put(final List<TransactionRecord> trs);

    public void remove(final List<Long> pks);

    public List<TransactionRecord> traverse(final long pk, final int nums);

    public long totalRecords();

    public long minPK();

    public long maxPK();

}
