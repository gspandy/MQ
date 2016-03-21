package com.ztesoft.zsmart.zmq.common.sysflag;

/**
 * Pull接口用到的flag定义
 * 
 * @author J.Wang
 *
 */
public class PullSysFlag {
	private final static int FLAG_COMMIT_OFFSET = 0x1 << 0; // 1
	private final static int FLAG_SUSPEND = 0x1 << 1; // 10
	private final static int FLAG_SUBSCRIPTION = 0x1 << 2;// 100
	private final static int FLAG_CLASS_FILTER = 0x1 << 3;// 1000

	public static int buildSysFlag(final boolean commitOffset, final boolean suspend, final boolean subscription,
			final boolean classFilter) {
		int flag = 0;

		if (commitOffset) {
			flag |= FLAG_COMMIT_OFFSET;
		}
		if (suspend) {
			flag |= FLAG_SUSPEND;
		}

		if (subscription) {
			flag |= FLAG_SUBSCRIPTION;
		}

		if (classFilter) {
			flag |= FLAG_CLASS_FILTER;
		}

		return flag;

	}
}
