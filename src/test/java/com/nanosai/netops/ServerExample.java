package com.nanosai.netops;

import com.nanosai.memops.bytes.BytesAllocatorAutoDefrag;
import com.nanosai.memops.objects.Bytes;
import com.nanosai.netops.iap.IapMessage;
import com.nanosai.netops.tcp.BytesBatch;
import com.nanosai.netops.tcp.IapMessageReaderFactory;
import com.nanosai.netops.tcp.TcpMessagePort;
import com.nanosai.netops.tcp.TcpServer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ServerExample {

    public static void main(String[] args) throws IOException {

        int tcpPort = 1111;
        //阻塞队列，放置socket连接
        BlockingQueue acceptedSocketQueue = new ArrayBlockingQueue(1024);
        TcpServer tcpServer = new TcpServer(tcpPort, acceptedSocketQueue);

        //Run TcpServer in its own thread. TcpServer listens for incoming connections, and puts the Sockets into a BlockingQueue,
        //for processing by you, or a TcpMessagePort.
        Thread tcpServerThread = new Thread(tcpServer);
        //启动监听线程，监听连接，有连接则将连接放入阻塞队列
        tcpServerThread.start();

        //启动selector,
        TcpMessagePort tcpMessagePort = createTcpMessagePort(acceptedSocketQueue);

        BytesBatch incomingMessageBatch = new BytesBatch(16, 64);

        // echo server message processing loop:

        while(true) {
            tcpMessagePort.addSocketsFromSocketQueue();


            //接收客户端发送过来的消息
            tcpMessagePort.readNow(incomingMessageBatch);

            for(int i=0; i < incomingMessageBatch.count; i++) {
                IapMessage incomingMessage =(IapMessage) incomingMessageBatch.blocks[i];

                //process message
                System.out.println("Processing message " + i);

                //create response
                //每条接收到的消息都封装发送消息
                IapMessage outgoingMessage = tcpMessagePort.getWriteMemoryBlock();
                outgoingMessage.allocate(1024);
                outgoingMessage.resetReadAndWriteIndexes();
                outgoingMessage.copyFrom(incomingMessage);
                incomingMessage.free();

                outgoingMessage.setTcpSocket(incomingMessage.getSourceTcpSocketId(), incomingMessage.getSourceTcpSocket());

                tcpMessagePort.writeNowOrEnqueue(outgoingMessage);


            }
            incomingMessageBatch.clear();
        }
    }

    private static TcpMessagePort createTcpMessagePort(BlockingQueue acceptedSocketQueue) throws IOException {
        IapMessageReaderFactory iapMessageReaderFactory = new IapMessageReaderFactory();

        BytesAllocatorAutoDefrag readMemoryAllocator  = new BytesAllocatorAutoDefrag(new byte[1024 * 1024]);
        BytesAllocatorAutoDefrag writeMemoryAllocator = new BytesAllocatorAutoDefrag(new byte[1024 * 1024]);

        return new TcpMessagePort(acceptedSocketQueue, iapMessageReaderFactory, readMemoryAllocator, writeMemoryAllocator);
    }

}
