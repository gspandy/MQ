package com.ztesoft.zsmart.zmq.remoting.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * 
 * 字段可以为空注解 <br> 
 *  
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.annotation <br>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD,ElementType.METHOD,ElementType.PARAMETER,ElementType.LOCAL_VARIABLE})
public @interface CFNullable {

}
