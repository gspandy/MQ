package com.ztesoft.zsmart.zmq.broker.mqtrace;

public interface ConsumeMessageHook {
    public String hookName();


    public void consumeMessageBefore(final ConsumeMessageContext context);


    public void consumeMessageAfter(final ConsumeMessageContext context);
}
