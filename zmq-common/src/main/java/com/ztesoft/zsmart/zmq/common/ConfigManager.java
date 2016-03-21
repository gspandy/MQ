package com.ztesoft.zsmart.zmq.common;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * 各种配置管理接口 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月21日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common <br>
 */
public abstract class ConfigManager {
    private static final Logger plog = LoggerFactory.getLogger(LoggerName.CommonLoggerName);

    public abstract String encode();

    public abstract String encode(final boolean prettyFormat);

    public abstract void decode(final String jsonString);

    public abstract String configFilePath();

    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);
            // 文件不存在，或者为空文件
            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            }
            else {
                this.decode(jsonString);
                plog.info("load {} OK", fileName);
                return true;
            }
        }
        catch (Exception e) {
            plog.error("load " + fileName + " Failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                plog.info("load " + fileName + " OK");
                return true;
            }
        }
        catch (Exception e) {
            plog.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            }
            catch (IOException e) {
                plog.error("persist file Exception, " + fileName, e);
            }
        }
    }
}
