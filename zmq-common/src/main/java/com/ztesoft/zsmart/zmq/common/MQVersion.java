package com.ztesoft.zsmart.zmq.common;

/**
 * 定义MQ 版本
 * 
 * @author J.Wang
 *
 */
public class MQVersion {
	public static final int CurrentVersion = Version.V3_2_6.ordinal();

	public static String getVersionDesc(int value) {
		try {
			Version v = Version.values()[value];
			return v.name();
		} catch (Exception e) {
		}
		return "HigherVersion";
	}

	public static enum Version {
		V3_2_6
	}
}
