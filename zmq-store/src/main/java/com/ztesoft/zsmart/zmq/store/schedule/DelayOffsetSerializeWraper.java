package com.ztesoft.zsmart.zmq.store.schedule;

import java.util.concurrent.ConcurrentHashMap;

import com.ztesoft.zsmart.zmq.remoting.protocol.RemotingSerializable;

/**
 * 延时消息进度 序列化包装
 * 
 * @author J.Wang
 *
 */
public class DelayOffsetSerializeWraper extends RemotingSerializable {
	private ConcurrentHashMap<Integer /* level */, Long /* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(
			32);

	public ConcurrentHashMap<Integer, Long> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(ConcurrentHashMap<Integer, Long> offsetTable) {
		this.offsetTable = offsetTable;
	}

}
