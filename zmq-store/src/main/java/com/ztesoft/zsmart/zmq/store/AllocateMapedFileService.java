package com.ztesoft.zsmart.zmq.store;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.ServiceThread;
import com.ztesoft.zsmart.zmq.common.UtilAll;
import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * Create MapedFile in advance <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月23日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public class AllocateMapedFileService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private static int WaitTimeOut = 1000 * 5;

    private ConcurrentHashMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<String, AllocateRequest>();

    private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<AllocateMapedFileService.AllocateRequest>();

    private volatile boolean hasException = false;

    public MapedFile putRequestAndReturnMapedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);

        boolean nextPutOk = (this.requestTable.putIfAbsent(nextFilePath, nextReq) == null);
        boolean nextNextPutOK = (this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null);

        if (nextPutOk) {
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }
        if (nextNextPutOK) {
            boolean offerOK = this.requestQueue.offer(nextNextReq);
            if (!offerOK) {
                log.warn("add a request to preallocate queue failed");
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(WaitTimeOut, TimeUnit.MILLISECONDS);

                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                }
                this.requestTable.remove(nextFilePath);
                return result.getMapedFile();
            }
            else {
                log.error("find preallocate mmap failed, this never happen");
            }
        }
        catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    @Override
    public String getServiceName() {
        return AllocateMapedFileService.class.getSimpleName();
    }

    public void shutdown() {
        this.stoped = true;
        this.thread.interrupt();

        try {
            this.thread.join(this.getJointime());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mapedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mapedFile.getFileName());
                req.mapedFile.destroy(1000);
            }
        }
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped() && this.mmapOperation())
            ;

        log.info(this.getServiceName() + " service end");
    }

    private boolean mmapOperation() {
        AllocateRequest req = null;
        try {
            req = this.requestQueue.take();
            if (null == this.requestTable.get(req.getFilePath())) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                    + req.getFileSize());
                return true;
            }

            if (req.getMapedFile() == null) {
                long beginTime = System.currentTimeMillis();
                MapedFile mapedFile = new MapedFile(req.getFilePath(), req.getFileSize());
                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mapedFile spent time(ms) " + eclipseTime + " queue size " + queueSize + " "
                        + req.getFilePath() + " " + req.getFileSize());
                }

                req.setMapedFile(mapedFile);
                this.hasException = false;
            }
        }
        catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception, maybe by shutdown");
            this.hasException = true;
            return false;
        }
        catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
        }
        finally {
            if (req != null)
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;

        private int fileSize;

        private CountDownLatch countDownLatch = new CountDownLatch(1);

        private volatile MapedFile mapedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MapedFile getMapedFile() {
            return mapedFile;
        }

        public void setMapedFile(MapedFile mapedFile) {
            this.mapedFile = mapedFile;
        }

        public int compareTo(AllocateRequest other) {
            return this.fileSize < other.fileSize ? 1 : this.fileSize > other.fileSize ? -1 : 0;
        }
    }
}
