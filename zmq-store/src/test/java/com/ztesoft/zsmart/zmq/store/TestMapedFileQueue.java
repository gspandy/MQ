package com.ztesoft.zsmart.zmq.store;

import java.util.concurrent.TimeUnit;

public class TestMapedFileQueue {

    public static void main(String[] args) throws InterruptedException {
         MapedFileQueue queue = new MapedFileQueue("d:/zmq/store/filequeue", 1 * 1024, null);
         
         for(int i=0;i<10;i++){
             long offset = i * 1024;
             MapedFile file = queue.getLastMapedFile();
             file.appendMessage(new byte[1024]);
         }
         
         //queue.commit(4);
         
         TimeUnit.SECONDS.sleep(10);

    }

}
