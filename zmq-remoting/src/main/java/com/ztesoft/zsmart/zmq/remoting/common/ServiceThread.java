package com.ztesoft.zsmart.zmq.remoting.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 后台服务线程抽象类
 * 
 * @author J.Wang
 *
 */
public abstract class ServiceThread implements Runnable {
	private static final Logger stLog = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
	protected final Thread thread;
	private static final long JoinTime = 90 * 1000;
	private volatile boolean hasNotified = false;
	private volatile boolean stoped = false;

	public ServiceThread() {
		this.thread = new Thread(this, this.getServiceName());
	}

	public abstract String getServiceName();

	public void start() {
		this.thread.start();
	}

	public void shutdown() {
		this.shutdown(false);
	}

	public void stop() {
		this.stop(false);
	}

	public void makeStop() {
		this.stoped = true;
		stLog.info("makestop thread" + this.getServiceName());
	}

	public void stop(final boolean interrupt) {
		this.stoped = true;
		stLog.info("stop thread" + this.getServiceName());
		synchronized (this) {
			if (!this.hasNotified) {
				this.hasNotified = true;
				this.notify();
			}
		}

		if (interrupt) {
			this.thread.interrupt();
		}
	}

	public void shutdown(final boolean interrupt) {
		this.stoped = true;
		stLog.info("shutdown thread" + this.getServiceName());
		synchronized (this) {
			if (!this.hasNotified) {
				this.hasNotified = true;
				this.notify();
			}
		}

		if (interrupt) {
			this.thread.interrupt();
		}
		long beginTime;
		long eclipseTime;
		try {
			beginTime = System.currentTimeMillis();
			this.thread.join(this.getJointime());
			eclipseTime = System.currentTimeMillis();
			stLog.info("join thread" + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " " + beginTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void wakeup() {
		synchronized (this) {
			if (!this.hasNotified) {
				this.hasNotified = true;
				this.notify();
			}
		}
	}

	protected void waitForRunning(long interval) {
		synchronized (this) {
			if (this.hasNotified) {
				this.hasNotified = false;
				this.onWaitEnd();
				return;
			}
			try {
				this.wait(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.hasNotified = false;
				this.onWaitEnd();
			}
		}
	}

	protected void onWaitEnd() {
	}

	public boolean isStoped() {
		return stoped;
	}

	public long getJointime() {
		return JoinTime;
	}
}
