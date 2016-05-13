package com.ztesoft.zsmart.zmq.store.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexFileTest {

    public static void main(String[] args) throws IOException {
        readWrite();
        
    }
    
    public static void testWrite() throws IOException{
        IndexFile indexFile = new IndexFile("d:/zmq/store/index/000",100, 50000000, 0, 0);
        long startTime = System.currentTimeMillis();
        System.out.println("write Index start:"+startTime);
        for(int i=0;i<50000000;i++){
            indexFile.putKey("index"+ (i % 4999), i, System.currentTimeMillis());
        }
        System.out.println("write Index end:"+(System.currentTimeMillis() - startTime));
        boolean putResult = indexFile.putKey(Long.toString(400), 5000000, System.currentTimeMillis());
        
        System.out.println(putResult);
    }
    
    public static void readWrite() throws IOException{
        IndexFile indexFile = new IndexFile("d:/zmq/store/index/000",100, 50000000, 0, 0);
        indexFile.load();
        
     // 读索引
        final List<Long> phyOffsets = new ArrayList<Long>();
        indexFile.selectPhyOffset(phyOffsets, "index60",10, 0, Long.MAX_VALUE, true);
        
        for (Long offset : phyOffsets) {
            System.out.println(offset);
        }
    }

}
