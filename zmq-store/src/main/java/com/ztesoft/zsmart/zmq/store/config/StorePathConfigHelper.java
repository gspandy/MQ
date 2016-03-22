package com.ztesoft.zsmart.zmq.store.config;

import java.io.File;

/**
 * 
 * 存储文件工具类<br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月22日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.store.config <br>
 */
public class StorePathConfigHelper {

    /**
     * consume path: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    /**
     * Store index: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    /**
     * checkpoint: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    /**
     * abort: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }

    /**
     * delay offset: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }

    /**
     * statetable: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getTranStateTableStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "statetable";
    }

    /**
     * redo: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param rootDir
     * @return <br>
     */
    public static String getTranRedoLogStorePath(final String rootDir) {
        return rootDir + File.separator + "transaction" + File.separator + "redolog";
    }

}
