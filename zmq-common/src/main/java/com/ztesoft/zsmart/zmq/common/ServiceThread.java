package com.ztesoft.zsmart.zmq.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * 后台服务线程基类 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common <br>
 */
public abstract class ServiceThread implements Runnable {

    private static final Logger stlog = LoggerFactory.getLogger(LoggerName.CommonLoggerName);

    // 执行线程
    protected final Thread thread;

    // 线程回收时间，默认90S
    private static final long JoinTime = 90 * 1000;

    // 是否已经被Notify过
    protected volatile boolean hasNotified = false;

    // 线程是否已经停止
    protected volatile boolean stoped = false;

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
        stlog.info("makestop thread " + this.getServiceName());
    }

    public void stop(final boolean interrupt) {
        this.stoped = true;
        stlog.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);
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
        stlog.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long eclipseTime = System.currentTimeMillis() - beginTime;
            stlog.info("join thread " + this.getServiceName() + " eclipse time(ms) " + eclipseTime + " "
                + this.getJointime());
        }
        catch (InterruptedException e) {
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
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
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
