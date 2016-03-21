package com.ztesoft.zsmart.zmq.common.stats;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.ztesoft.zsmart.zmq.common.UtilAll;

public class MomentStatsItemSet {
	private final ConcurrentHashMap<String, MomentStatsItem> statsItemTable = new ConcurrentHashMap<String, MomentStatsItem>(
			128);

	private final String statsName;
	private final ScheduledExecutorService scheduledExecutorService;
	private final Logger log;

	public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, Logger log) {
		super();
		this.statsName = statsName;
		this.scheduledExecutorService = scheduledExecutorService;
		this.log = log;

		init();
	}

	public MomentStatsItem getAndCreateStatsItem(final String statsKey) {
		MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
		if (statsItem == null) {
			statsItem = new MomentStatsItem(this.statsName, statsKey, scheduledExecutorService, log);
			MomentStatsItem prev = this.statsItemTable.put(statsKey, statsItem);
		}
		return statsItem;
	}

	public void setValue(final String statsKey, final int value) {
		MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
		statsItem.getValue().set(value);
	}

	public void init() {
		// 分钟整点执行
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					printAtMinutes();
				} catch (Throwable e) {
				}
			}
		}, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //
				1000 * 60 * 5, TimeUnit.MILLISECONDS);
	}

	private void printAtMinutes() {
		Iterator<Entry<String, MomentStatsItem>> it = this.statsItemTable.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, MomentStatsItem> next = it.next();
			next.getValue().printAtMinutes();
		}
	}
}
