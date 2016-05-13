package com.ztesoft.zsmart.zmq.store;

import java.io.IOException;

public class MapedFileTest {

    private static final String StoreMessage = "Once, there was a chance for me!";
    
    public static void main(String[] args) throws IOException {
        
        MapedFile mapedFile = new MapedFile("d:/zmq/store/0000",  1024 * 64);
        
        mapedFile.appendMessage(StoreMessage.getBytes());
        
        System.out.println("write OK");
        mapedFile.commit(3);
        
        SelectMapedBufferResult result = mapedFile.selectMapedBuffer(0);
        
        byte[] data = new byte[StoreMessage.length()];
        result.getByteBuffer().get(data);
        String readString = new String(data);

        System.out.println("Read: " + readString);
        
        // 禁止Buffer读写
        mapedFile.shutdown(1000);
        
     // mapedFile对象不可用
        System.out.println(mapedFile.isAvailable());
        
        // 释放读到的Buffer
        result.release();

        // 内存真正释放掉
        System.out.println(mapedFile.isCleanupOver());
    }

}
