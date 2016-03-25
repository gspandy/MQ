package com.ztesoft.zsmart.zmq.client.producer;

import com.ztesoft.zsmart.zmq.common.message.MessageExt;

public interface TransactionCheckListener {
    LocalTransactionState checkLocalTransactionState(final MessageExt ext);
}
