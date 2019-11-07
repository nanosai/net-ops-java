package com.nanosai.netops.iap;

import com.nanosai.memops.bytes.BytesAllocatorAutoDefrag;
import com.nanosai.memops.objects.Bytes;
import com.nanosai.memops.objects.ObjectPool;
import com.nanosai.netops.codec.RionCodec;
import com.nanosai.rionops.rion.write.RionWriter;

import com.nanosai.netops.tcp.TcpMessagePort;
import com.nanosai.netops.tcp.TcpSocket;
import com.nanosai.rionops.rion.read.RionReader;

import java.nio.ByteBuffer;

/**
 * Created by jjenkov on 03/02/2018.
 */
public class IapMessage extends Bytes {

    private long      sourceTcpSocketId;
    private TcpSocket sourceTcpSocket;
    private TcpMessagePort sourceTcpMessagePort;

    private long secureChannelId = 0;

    private RionReader rionReader = new RionReader();
    private RionWriter rionWriter = new RionWriter();
    private IapMessageBase messageBase = new IapMessageBase();

    private IapMessage next;
    private IapMessage prev;

    private RionCodec messageExtension;


    public IapMessage(BytesAllocatorAutoDefrag memoryAllocator, ObjectPool<Bytes> objectPool) {

        init(memoryAllocator, objectPool);
    }

    public void setTcpSocket(IapMessage message){
        setTcpSocket(message.getSourceTcpSocketId(), message.getSourceTcpSocket());
    }

    public void setTcpSocket(long sourceTcpSocketId, TcpSocket sourceTcpSocket){
        this.sourceTcpSocketId = sourceTcpSocketId;
        this.sourceTcpSocket   = sourceTcpSocket;
    }

    public long getSourceTcpSocketId() {
        return sourceTcpSocketId;
    }

    public TcpSocket getSourceTcpSocket() {
        return sourceTcpSocket;
    }


    public void initRead() {
        this.rionReader.setSource(this);
        this.rionReader.nextParse().moveInto().nextParse(); //move into Ion Object containing message
        this.messageBase.read(this.rionReader);
    }

    public void initWrite() {
        this.rionWriter.setDestination(this);
    }

    public RionReader getRionReader() {
        return rionReader;
    }

    public IapMessageBase getMessageBase() {
        return messageBase;
    }

    public void setNext(IapMessage next){
        this.next = next;
        next.prev = this;
    }

    public void setPrev(IapMessage prev){
        this.prev = prev;
        prev.next = this;
    }

    public void writeLeadByte(int leadByte){
        this.data[this.writeIndex++] = (byte) (255 & (leadByte));
    }


    //todo does this method really belong here? IAP specific code!
    public void writeLength(int length, int lengthLength){
        for(int i=(lengthLength -1) * 8; i>=0; i-=8){
            this.data[this.writeIndex++] = (byte) (255 & (length >> i));
        }
    }

    //todo does this method really belong here? Should it be renamed to just "write" ?
    public void writeValue(ByteBuffer byteBuffer, int length){
        byteBuffer.get(this.data, this.writeIndex, length);
        this.writeIndex += length;
    }

    public void copyFrom(Bytes source){
        System.arraycopy(
                source.data, source.startIndex,
                this  .data, this.writeIndex,
                source.lengthWritten()
        );
        this.writeIndex += source.lengthWritten();

    }

    public void resetReadAndWriteIndexes() {
        this.writeIndex = this.startIndex;
        this.readIndex  = this.startIndex;
    }



}
