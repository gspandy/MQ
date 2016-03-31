package com.ztesoft.zsmart.zmq.broker.pagecache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.ztesoft.zsmart.zmq.store.SelectMapedBufferResult;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {

    private final ByteBuffer byteBufferHeader;

    private final SelectMapedBufferResult selectMapedBufferResult;

    private long transfered;// the bytes which was transfered already

    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMapedBufferResult selectMapedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMapedBufferResult = selectMapedBufferResult;
    }

    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMapedBufferResult.getByteBuffer().limit();

    }

    @Override
    public long position() {

        return this.byteBufferHeader.position() + this.selectMapedBufferResult.getByteBuffer().position();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            this.transfered += target.write(this.byteBufferHeader);
        }
        else if (this.selectMapedBufferResult.getByteBuffer().hasRemaining()) {
            transfered += target.write(this.selectMapedBufferResult.getByteBuffer());
            return transfered;
        }
        return 0;

    }

    public void close() {
        this.deallocate();
    }

    @Override
    public long transfered() {
        return transfered;
    }

    @Override
    protected void deallocate() {
        this.selectMapedBufferResult.release();
    }

}
