package com.ztesoft.zsmart.zmq.client;

import com.ztesoft.zsmart.zmq.common.UtilAll;

public class VirtualEnvUtil {
    private static final String VIRTUAL_APPGROUP_PREFIX = "%%PROJECT_%s%%";

    public static String buildWithProjectGroup(String origin, String projectGroup) {
        if (!UtilAll.isBlank(projectGroup)) {
            String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
            if (!origin.endsWith(prefix)) {
                return origin + prefix;
            }
        }
        return origin;
    }

    /**
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String clearProjectGroup(String origin, String projectGroup) {
        String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
        if (!UtilAll.isBlank(prefix) && origin.endsWith(prefix)) {
            return origin.substring(0, origin.lastIndexOf(prefix));
        }
        else {
            return origin;
        }
    }
}
