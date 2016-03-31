package com.ztesoft.zsmart.zmq.broker.client;

import java.util.List;

import io.netty.channel.Channel;

public interface ConsumerIdsChangeListener {
    public void consumerIdsChanged(final String group, final List<Channel> channels);
}
