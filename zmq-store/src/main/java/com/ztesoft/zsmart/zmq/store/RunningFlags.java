package com.ztesoft.zsmart.zmq.store;

/**
 * 存储模型运行过程的状态位<br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月24日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store <br>
 */
public class RunningFlags {
    // 禁止读权限
    private static final int NotReadableBit = 1; // 1

    // 禁止写权限
    private static final int NotWriteableBit = 1 << 1; // 10 2

    // 逻辑队列是否发生错误
    private static final int WriteLogicsQueueErrorBit = 1 << 2;// 100 4

    // 索引文件是否发生错误
    private static final int WriteIndexFileErrorBit = 1 << 3;// 1000 8

    // 磁盘空间不足
    private static final int DiskFullBit = 1 << 4;// 10000 16

    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            this.flagBits &= ~NotReadableBit;
        }
        return result;
    }

    public boolean isReadable() {
        if ((this.flagBits & NotReadableBit) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            this.flagBits |= NotReadableBit;
        }
        return result;
    }

    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            this.flagBits &= ~NotWriteableBit;
        }
        return result;
    }

    public boolean isWriteable() {
        if ((this.flagBits & (NotWriteableBit | WriteLogicsQueueErrorBit | DiskFullBit | WriteIndexFileErrorBit)) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NotWriteableBit;
        }
        return result;
    }

    public void makeLogicsQueueError() {
        this.flagBits |= WriteLogicsQueueErrorBit;
    }

    public boolean isLogicsQueueError() {
        if ((this.flagBits & WriteLogicsQueueErrorBit) == WriteLogicsQueueErrorBit) {
            return true;
        }

        return false;
    }

    public void makeIndexFileError() {
        this.flagBits |= WriteIndexFileErrorBit;
    }

    public boolean isIndexFileError() {
        if ((this.flagBits & WriteIndexFileErrorBit) == WriteIndexFileErrorBit) {
            return true;
        }

        return false;
    }

    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits |= DiskFullBit;
        return result;
    }

    /**
     * 返回Disk是否正常
     */
    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DiskFullBit) == DiskFullBit);
        this.flagBits &= ~DiskFullBit;
        return result;
    }
}
