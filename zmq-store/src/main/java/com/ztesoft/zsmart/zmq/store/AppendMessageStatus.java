package com.ztesoft.zsmart.zmq.store;

/**
 * When write a message to the commit log, returns code
 * @author J.Wang
 *
 */
public enum AppendMessageStatus {
	PUT_OK,
	END_OF_FILE,
	MESSAGE_SIZE_EXCEEDED,
	UNKNOWN_ERROR
}
