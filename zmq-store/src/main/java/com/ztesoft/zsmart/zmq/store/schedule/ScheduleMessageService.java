package com.ztesoft.zsmart.zmq.store.schedule;

import java.util.ConcurrentModificationException;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.ConfigManager;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

public class ScheduleMessageService extends ConfigManager {

	public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
	private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
	private static final long FIRST_DELAY_TIME = 1000L;
	private static final long DELAY_FOR_A_WHILE = 100L;
	private static final long DELAY_FOR_A_PERIOD = 10000L;

	// 每个level对应的延时时间
	private final ConcurrentHashMap<Integer /* level */, Long/*
																 * delay
																 * timeMillis
																 */> delayLevelTable = new ConcurrentHashMap<Integer, Long>(
			32);
	// 延时计算到了哪里
	private final ConcurrentHashMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(
			32);
	// 定时器
	private final Timer timer = new Timer("ScheduleMessageTimerThread", true);

	//
	private final DefaultMessageStore defaultMessageStore;

	// 最大值
	private int maxDelayLevel;

	@Override
	public String encode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String encode(boolean prettyFormat) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void decode(String jsonString) {
		// TODO Auto-generated method stub

	}

	@Override
	public String configFilePath() {
		// TODO Auto-generated method stub
		return null;
	}

}
