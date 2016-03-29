package com.ztesoft.zsmart.zmq.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ztesoft.zsmart.zmq.store.index.IndexFile;

public class IndexFileTest {
    private final int hashSlotNum = 100;

    private final int indexNum = 400;

    @Test
    public void test_put_index() {
        try {
            IndexFile file = new IndexFile("100", hashSlotNum, indexNum, 0, 0);

            // 写入索引
            for (long i = 0; i < 399; i++) {
                boolean putResult = file.putKey(Long.toString(i), i, System.currentTimeMillis());
                assertTrue(putResult);
            }
            
            // 索引文件已经满了， 再写入会失败
            boolean putResult = file.putKey(Long.toString(400), 400, System.currentTimeMillis());
            assertFalse(putResult);
            
         // 读索引
            final List<Long> phyOffsets = new ArrayList<Long>();
            file.selectPhyOffset(phyOffsets, "1", 401, 0, Long.MAX_VALUE, true);
            for (Long offset : phyOffsets) {
                System.out.println(offset);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

}
