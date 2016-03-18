package com.ztesoft.zsmart.zmq.remoting;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingCommand;

public interface RPCHook {
    public void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    public void doAfterResponse(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);
}
