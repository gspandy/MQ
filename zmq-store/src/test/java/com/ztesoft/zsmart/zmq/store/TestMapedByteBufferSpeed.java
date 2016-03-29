package com.ztesoft.zsmart.zmq.store;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

public class TestMapedByteBufferSpeed {

    public static void main(String[] args) throws Exception {
//         normalWrite();
//
//        mappedWrite();
        write();
    }
    
    
    public static void write() throws Exception{
        RandomAccessFile out = new RandomAccessFile("d:/map.data", "rw");

        long size = Integer.MAX_VALUE -1;
        MappedByteBuffer mapp = out.getChannel().map(MapMode.READ_WRITE, 0, MapedFile.OS_PAGE_SIZE);

        mapp.putLong(System.currentTimeMillis());
        System.out.println(mapp.position());
        mapp.putLong(System.currentTimeMillis());
        System.out.println(mapp.position());
        mapp.putLong(System.currentTimeMillis());
        System.out.println(mapp.position());
        mapp.force();
    }
    

    public static void normalWrite() throws Exception {
        System.out.println("normal write 1G start:");
        long startTime = System.currentTimeMillis();
        FileOutputStream out = new FileOutputStream("d:/normal.data");
        try {
            long size = Integer.MAX_VALUE -1;

            // 每次写入512
            long position = 0;
            while (position < size) {
                position += 512;
                out.write(new byte[512]);
            }
        }
        finally {
            if (out != null) {
                out.flush();
                out.close();
            }
        }

        System.out.println("normal write 1G :" + (System.currentTimeMillis() - startTime));
    }

    public static void mappedWrite() throws Exception {
        System.out.println("mapped write 1G start:");
        long startTime = System.currentTimeMillis();
        RandomAccessFile out = new RandomAccessFile("d:/map.data", "rw");

        long size = Integer.MAX_VALUE -1;
        MappedByteBuffer mapp = out.getChannel().map(MapMode.READ_WRITE, 0, size);

        // 每次写入512
        long position = 0;
        while (position < size) {
            position += 512L;
            mapp.wrap(new byte[512]);
        }

        mapp.force();

        System.out.println("mapped write 1G :" + (System.currentTimeMillis() - startTime));
    }

}
