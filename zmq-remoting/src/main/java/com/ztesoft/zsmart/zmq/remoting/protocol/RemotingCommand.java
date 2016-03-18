package com.ztesoft.zsmart.zmq.remoting.protocol;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.annotation.JSONField;
import com.ztesoft.zsmart.zmq.remoting.CommandCustomHeader;
import com.ztesoft.zsmart.zmq.remoting.annotation.CFNotNull;
import com.ztesoft.zsmart.zmq.remoting.exception.RemotingCommandException;

/**
 * Remoting模块中 服务器与客户端通过传递RemotingCommand来交互 <br>
 * 
 * @author wang.jun<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2016年3月18日 <br>
 * @since V7.3<br>
 * @see com.ztesoft.zsmart.zmq.remoting.protocol <br>
 */
public class RemotingCommand {

    // remoting protocol version
    public static String RemotingVersionKey = "zmq.remoting.version";

    private static volatile int ConfigVersion = -1;

    private static AtomicInteger RequestId = new AtomicInteger(0);

    // 0 REQUEST_COMMAND 1 RESPONSE_COMMAND
    private static final int RPC_TYPE = 0;

    // 0 RPC 1 ONEWAY
    private static final int RPC_ONEWAY = 1;

    // protocol header part

    private int code;

    private LanguageCode language = LanguageCode.JAVA;

    private int version = 0;

    private int opaque = RequestId.getAndIncrement();

    private int flag = 0;

    private String remark;

    private HashMap<String, String> extFields;

    private transient CommandCustomHeader customHeader;

    // 消息体
    private transient byte[] body;

    /**
     * 防止其它包 实例化
     */
    protected RemotingCommand() {

    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    /**
     * 只有通信层内部会调用，业务不会调用: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param code
     * @param remark
     * @param classheader
     * @return <br>
     */
    public static RemotingCommand createResponseCommand(int code, String remark,
        Class<? extends CommandCustomHeader> classheader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classheader != null) {

            try {
                CommandCustomHeader objectHeader = classheader.newInstance();
                cmd.customHeader = objectHeader;
            }
            catch (Exception e) {
                return null;
            }

        }

        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {
        if (ConfigVersion >= 0) {
            cmd.setVersion(ConfigVersion);
        }
        else {
            String v = System.getProperty(RemotingVersionKey);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                ConfigVersion = value;
            }

        }
    }

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = this.customHeader.getClass().getDeclaredFields();

            if (null == this.extFields) {
                this.extFields = new HashMap<String, String>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {

                    Object value = null;

                    try {
                        PropertyDescriptor descriptor = new PropertyDescriptor(field.getName(), customHeader.getClass());
                        value = descriptor.getReadMethod().invoke(customHeader, null);
                    }
                    catch (Exception e) {
                    }

                    if (value != null) {
                        this.addExtFields(field.getName(), value.toString());
                    }

                }
            }
        }
    }

    private static final String StringName = String.class.getCanonicalName();

    private static final String IntegerName1 = Integer.class.getCanonicalName();

    private static final String IntegerName2 = int.class.getCanonicalName();

    private static final String LongName1 = Long.class.getCanonicalName();

    private static final String LongName2 = long.class.getCanonicalName();

    private static final String BooleanName1 = Boolean.class.getCanonicalName();

    private static final String BooleanName2 = boolean.class.getCanonicalName();

    private static final String DoubleName1 = Double.class.getCanonicalName();

    private static final String DoubleName2 = double.class.getCanonicalName();

    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
        throws RemotingCommandException {
        if (this.extFields != null) {
            CommandCustomHeader objectHeader;
            try {
                objectHeader = classHeader.newInstance();
            }
            catch (InstantiationException e) {
                return null;
            }
            catch (IllegalAccessException e) {
                return null;
            }

            // 检查返回对象是否有效
            Field[] fields = classHeader.getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        String value = this.extFields.get(fieldName);
                        if (value == null) {
                            Annotation annotation = field.getAnnotation(CFNotNull.class);
                            if (annotation != null) {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                            }

                            continue;
                        }

                        try {
                            PropertyDescriptor descriptor = new PropertyDescriptor(field.getName(), classHeader);
                            descriptor.getWriteMethod().invoke(objectHeader, value);
                        }
                        catch (Exception e) {
                        }
                    }
                }
            }
            objectHeader.checkFields();

            return objectHeader;
        }
        return null;
    }

    private byte[] buildHeader() {
        this.makeCustomHeaderToNet();
        return RemotingSerializable.encode(this);
    }

    public ByteBuffer encode() {

        // 1 header length size
        int length = 4;

        // 2 header data length
        byte[] headerData = this.buildHeader();
        length += headerData.length;

        // 3 body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.putInt(headerData.length);

        // header data
        result.put(headerData);

        // body data
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    /**
     * 只打包Header ,body 部分独产传输: <br>
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param bodyLength
     * @return <br>
     */
    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.buildHeader();
        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.putInt(headerData.length);

        // header data
        result.put(headerData);

        result.flip();

        return result;
    }

    public static RemotingCommand decode(final byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    /**
     * protocol 解析: <br>
     * | length | headlength | headdata | bodydata | 4 | 4 | |
     * 
     * @author wang.jun<br>
     * @taskId <br>
     * @param byteBuffer
     * @return <br>
     */
    public static RemotingCommand decode(final ByteBuffer byteBuffer) {
        int length = byteBuffer.getInt();
        
        int headerLength = byteBuffer.getInt();

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        int bodyLength = length - 4 - headerLength;

        byte[] bodyData = null;

        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }

        RemotingCommand cmd = RemotingSerializable.decode(headerData, RemotingCommand.class);
        cmd.body = bodyData;

        return cmd;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public static int createNewRequestId() {
        return RequestId.incrementAndGet();
    }

    public void addExtFields(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<String, String>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque="
            + opaque + ", flag(B)=" + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields
            + "]";
    }
}
