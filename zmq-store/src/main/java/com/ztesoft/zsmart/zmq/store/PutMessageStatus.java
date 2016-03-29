package com.ztesoft.zsmart.zmq.store;

/**
 * 写入消息过程的返回结果
 * @author J.Wang
 *
 */
public enum PutMessageStatus {
	PUT_OK,
	FLUSH_DISK_TIMEOUT,
	FLUSH_SLAVE_TIMEOUT,
	SLAVE_NOT_AVAILABLE,
	SERVICE_NOT_AVAILABLE,
	CREATE_MAPEDFILE_FAILED,
	MESSAGE_ILLEGAL,
	UNKNOWN_ERROR
}
