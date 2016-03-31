package com.ztesoft.zsmart.zmq.broker.filter;

import org.slf4j.Logger;

public class FilterServerUtil {

    private static String[] splitShellString(final String shellString) {
        String[] split = shellString.split(" ");
        return split;
    }

    public static void callShell(final String shellString, final Logger log) {
        Process process = null;

        try {
            String[] cmdArray = splitShellString(shellString);
            process = Runtime.getRuntime().exec(cmdArray);
            process.waitFor();
            log.info("callShell: <{}> OK", shellString);
        }
        catch (Exception e) {
            log.error("callShell: readLine IOException, " + shellString, e);
        }
        finally {
            if (null != process)
                process.destroy();
        }
    }

}
