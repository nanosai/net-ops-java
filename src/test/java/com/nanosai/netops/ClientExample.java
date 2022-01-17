package com.nanosai.netops;

import com.nanosai.memops.bytes.BytesAllocatorAutoDefrag;
import com.nanosai.netops.iap.IapMessage;
import com.nanosai.netops.iap.IapMessageBase;
import com.nanosai.netops.tcp.BytesBatch;
import com.nanosai.netops.tcp.IapMessageReaderFactory;
import com.nanosai.netops.tcp.TcpMessagePort;
import com.nanosai.netops.tcp.TcpSocket;
import com.nanosai.rionops.rion.write.RionWriter;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ClientExample {

    public static void main(String[] args) throws IOException {

        //初始化连接队列queue

        //初始化 消息读取 工厂对象
        //初始化读写内存分配器 BytesAllocatorAutoDefrag
        //初始化 消息读写池pool
        // this.readObjectPool = new ObjectPool<IapMessage>(1024, new IapMessageFactory(this.readBytesAllocator));

        //打开读写的selector
        //初始化读写buffer
        final TcpMessagePort socketsPort = createTcpMessagePort(new ArrayBlockingQueue(1024));

        //与服务端连接，绑定服务端地址
        // 初始化与服务器连接的channel,向读的selector注册
        // 使用channel初始化tcp socket ，tcpSocket绑定了多个对象，如messageReader，内存分配器
        TcpSocket tcpSocket = socketsPort.addSocket("localhost", 1111);

        RionWriter rionWriter = new RionWriter().setNestedFieldStack(new int[2]);

        //封装服务端返回的结果
        BytesBatch responses = new BytesBatch(10);

        int counter = 0;
        while(true){
            //封装发送的消息
            IapMessage request = socketsPort.getWriteMemoryBlock();
            request.allocate(1024);
            request.resetReadAndWriteIndexes();

            int freeWriteBlockStartIndex = socketsPort.getWriteBytesAllocator().freeBlockStartIndex(0);
            int freeWriteBlockEndIndex = socketsPort.getWriteBytesAllocator().freeBlockEndIndex(0);

            int freeReadBlockStartIndex = socketsPort.getReadBytesAllocator().freeBlockStartIndex(0);
            int freeReadBlockEndIndex = socketsPort.getReadBytesAllocator().freeBlockEndIndex(0);

            generateIAPMessage(request, rionWriter);

            request.setTcpSocket(0, tcpSocket);

            System.out.println("Sending message " + counter);
            //向写selector注册channel,并且将待发送的消息放入队列或直接发送
            socketsPort.writeNowOrEnqueue(request);

            //发送消息，并且取消写selector上面的写注册，向取出消息队列的消息，放入写buffer,然后删除消息队列的消息，将消息从buffer中写入channel
            socketsPort.writeBlock();
            System.out.println("Message Sent");
            //request.free();


            //sleep(100);

            //try reading from socketsProxy
            //接收服务器返回的消息
            System.out.println("Receiving responses");
            int messagesRead = socketsPort.readBlock(responses);

            System.out.println("messagesRead = " + messagesRead);

            for(int i=0; i<responses.count; i++) {
                IapMessage iapMessage = (IapMessage) responses.blocks[i];

                System.out.println("  Processing response " + i);
                iapMessage.free();
            }

            System.out.println("");

            sleep(200);
            responses.clear();

            counter++;
        }

        //todo convenience method for MemoryBlock's as destination
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void generateIAPMessage(IapMessage request, RionWriter ionWriter) {
        ionWriter.setDestination(request);

        ionWriter.writeObjectBeginPush(1);

        IapMessageBase messageBase = new IapMessageBase();
        messageBase.setReceiverNodeId         (new byte[]{33});
        messageBase.setSemanticProtocolId     (new byte[]{22});
        messageBase.setSemanticProtocolVersion(new byte[] {0});
        messageBase.setMessageType            (new byte[]{11});

        messageBase.write(ionWriter);

        ionWriter.writeObjectEndPop();

        request.writeIndex = ionWriter.index;

        System.out.println("length = " + request.lengthWritten());
    }

    private static void generateIAPMessage2(IapMessage request, RionWriter writer) {
        writer.setDestination(request); //resets RionWriter



    }



    private static TcpMessagePort createTcpMessagePort(BlockingQueue acceptedSocketQueue) throws IOException {
        IapMessageReaderFactory iapMessageReaderFactory = new IapMessageReaderFactory();

        BytesAllocatorAutoDefrag readMemoryAllocator  = new BytesAllocatorAutoDefrag(new byte[1024 * 1024]);
        BytesAllocatorAutoDefrag writeMemoryAllocator = new BytesAllocatorAutoDefrag(new byte[1024 * 1024]);

        return new TcpMessagePort(acceptedSocketQueue, iapMessageReaderFactory, readMemoryAllocator, writeMemoryAllocator);
    }
}
