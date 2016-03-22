package com.ztesoft.zsmart.zmq.store;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ztesoft.zsmart.zmq.common.constant.LoggerName;

/**
 * Pagecache文件访问封装
 * 
 * @author J.Wang
 *
 */
public class MapedFile extends ReferenceResource {

	public static final int OS_PAGE_SIZE = 1024 * 4;
	private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
	// 当前JVM中映射的虚拟内存总大小
	private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);
	// 当前JVM中mmap句柄数量
	private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);
	// 映射文件名
	private final String fileName;
	// 映射的超始偏移量
	private final long fileFromOffset;
	// 映射的文件大小定长
	private final int fileSize;
	// 映射的文件
	private final File file;
	// 映射的内存对象，position永远不变
	private final MappedByteBuffer mappedByteBuffer;
	// 当前写到什么位置
	private final AtomicInteger wrotePosition = new AtomicInteger(0);
	// Flush到什么位置
	private final AtomicInteger committedPosition = new AtomicInteger(0);
	// 映射的FileChannel对象
	private FileChannel channel;
	// 最后一条消息存储时间
	private volatile long storeTimestamp = 0;
	private boolean firstCreateInQueue = false;

	public static void ensureDirOK(final String dirName) {
		if (dirName != null) {
			File f = new File(dirName);
			if (!f.exists()) {
				boolean result = f.mkdirs();
				log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
			}
		}
	}

	@Override
	public boolean cleanup(long currentRef) {
		return false;
	}

}
