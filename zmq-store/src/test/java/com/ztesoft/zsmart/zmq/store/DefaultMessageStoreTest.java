package com.ztesoft.zsmart.zmq.store;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import com.ztesoft.zsmart.zmq.store.config.FlushDiskType;
import com.ztesoft.zsmart.zmq.store.config.MessageStoreConfig;

public class DefaultMessageStoreTest {
    // 队列个数
    private static int QUEUE_TOTAL = 100;

    // 发往哪个队列
    private static AtomicInteger QueueId = new AtomicInteger(0);

    // 发送主机地址
    private static SocketAddress BornHost;

    // 存储主机地址
    private static SocketAddress StoreHost;

    // 消息体
    private static byte[] MessageBody;

    private static final String StoreMessage = "Once, there was a chance for me!";
    
    static {
        try {
            StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
            BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        }
        catch (Exception e) {
        }

    }
    

    public static MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("AAA");
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(4);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);

        return msg;
    }
    
    
    public static void test_group_commit() throws Exception{
        System.out.println("================================================================");
        long totalMsgs = 10000;
        QUEUE_TOTAL = 1;

        // 构造消息体
        MessageBody = StoreMessage.getBytes();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        // 每个物理映射文件 4K
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 4);
        messageStoreConfig.setStorePathRootDir("d:/zmq/store");
        
        // 开启GroupCommit功能
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        
        MessageStore master = new DefaultMessageStore(messageStoreConfig, null);
        // 第一步，load已有数据
        boolean load = master.load();
        
     // 第二步，启动服务
        master.start();
        for (long i = 0; i < totalMsgs; i++) {
            PutMessageResult result = master.putMessage(buildMessage());

        }
        
     // 开始读文件
//        for (long i = 0; i < totalMsgs; i++) {
//            try {
//                GetMessageResult result = master.getMessage("GROUP_A", "TOPIC_A", 0, i, 1024 * 1024, null);
//                if (result == null) {
//                    System.out.println("result == null " + i);
//                }
//                 
//                result.release();
//                System.out.println("read " + i + " OK");
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }

        // 关闭存储服务
        //master.shutdown();

        // 删除文件
       // master.destroy();
        System.out.println("================================================================");
        
    }

    public static void main(String[] args) throws Exception {
         
        test_group_commit();
    }

}
