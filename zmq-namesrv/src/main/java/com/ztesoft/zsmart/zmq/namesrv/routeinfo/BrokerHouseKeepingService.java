package com.ztesoft.zsmart.zmq.namesrv.routeinfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;
import com.ztesoft.zsmart.zmq.namesrv.NamesrvController;
import com.ztesoft.zsmart.zmq.remoting.ChannelEventListener;

public class BrokerHouseKeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    //private final NamesrvController namesrvController;

    public BrokerHousekeepingService(NamesrvController namesrvController) {
      //  this.namesrvController = namesrvController;
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        

    }

    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        // TODO Auto-generated method stub

    }

}
