package com.ztesoft.zsmart.zmq.common.utils;

/**
 * 
 * 字符串工具类 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月22日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.common.utils <br>
 */
public class StringUtils {
    /**
     * 
     * 判断字符串是否为数字: <br> 
     *  
     * @author wang.jun<br>
     * @taskId <br>
     * @param str
     * @return <br>
     */
    public static boolean isNumber(String str) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("[0-9]*");
        java.util.regex.Matcher match = pattern.matcher(str);
        if (match.matches() == false) {
            return false;
        }
        else {
            return true;
        }
    }
}
