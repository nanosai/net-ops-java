package com.nanosai.netops.tcp;

import com.nanosai.memops.bytes.BytesAllocatorAutoDefrag;

import com.nanosai.memops.objects.ObjectPool;
import com.nanosai.netops.iap.IapMessage;
import com.nanosai.netops.iap.IapMessageFactory;


import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by jjenkov on 06-05-2016.
 */
//socket管理、读写管理、流关闭管理
public class TcpMessagePort {

    // **************************
    // socket management related variables.
    // **************************
    //socket队列
    private BlockingQueue<SocketChannel> socketQueue    = null;
    private Map<Long, TcpSocket>         socketMap      = new HashMap<>(); //todo replace with faster Long, Object map.
    private List<SocketChannel>          newSocketsTemp = new ArrayList<SocketChannel>();

    private long nextSocketId = 1;

    private ISocketManager socketManager = null;


    // **************************
    // Read oriented variables.
    // **************************
    private IMessageReaderFactory messageReaderFactory = null;

    private Selector        readSelector = null;
    private ByteBuffer      readBuffer   = null;

    private TcpSocketPool tcpObjectPool       = new TcpSocketPool(1024);  //todo make size configurable.
    private BytesAllocatorAutoDefrag readBytesAllocator = null;
    private ObjectPool<IapMessage> readObjectPool = null;

    private TcpSocket[] readySocketsTemp = new TcpSocket[128]; //todo maybe change later to TCPSocketProxy - if that class is added.



    // ***************************
    // Write oriented variables.
    // ***************************
    private Selector   writeSelector   = null;
    private ByteBuffer writeByteBuffer = null;

    private List<TcpSocket> nonEmptyToEmptySockets = new ArrayList<>();

    private BytesAllocatorAutoDefrag writeBytesAllocator = null;
    private ObjectPool<IapMessage> writeObjectPool = null;




    // ***************************
    // TCP Socket closeOutputStream oriented variables.
    // ***************************
    private List<TcpSocket> socketsToBeClosed = new ArrayList<>();


    public TcpMessagePort(BlockingQueue<SocketChannel> socketQueue, IMessageReaderFactory messageReaderFactory,
                          BytesAllocatorAutoDefrag inboundMessageAllocator, BytesAllocatorAutoDefrag outboundMessageAllocator) throws IOException {
        this.socketQueue          = socketQueue;
        this.messageReaderFactory = messageReaderFactory;
        this.readBytesAllocator   = inboundMessageAllocator;
        this.writeBytesAllocator  = outboundMessageAllocator;
        this.readObjectPool       = new ObjectPool<IapMessage>(1024, new IapMessageFactory(this.readBytesAllocator));
        this.writeObjectPool      = new ObjectPool<IapMessage>(1024, new IapMessageFactory(this.writeBytesAllocator));
        init();
    }

    public TcpMessagePort(BlockingQueue<SocketChannel> socketQueue, IMessageReaderFactory messageReaderFactory) throws IOException {
        this(socketQueue,
             messageReaderFactory,
             new BytesAllocatorAutoDefrag(new byte[36 * 1024 * 1024] ) ,
             new BytesAllocatorAutoDefrag(new byte[36 * 1024 * 1024] )
        );
    }

    public BytesAllocatorAutoDefrag getReadBytesAllocator()  {
        return readBytesAllocator;
    }
    public BytesAllocatorAutoDefrag getWriteBytesAllocator() {
        return writeBytesAllocator;
    }

    public void setSocketManager(ISocketManager socketManager) {
        this.socketManager = socketManager;
        this.socketManager.init(this);
    }


    private void init() throws IOException {
        this.readSelector         = Selector.open();
        this.readBuffer           = ByteBuffer.allocate(1024 * 1024);

        this.writeSelector        = Selector.open();
        this.writeByteBuffer      = ByteBuffer.allocate(1024 * 1024);
    }



    public void addSocketsFromSocketQueue() throws IOException {
        socketQueue.drainTo(this.newSocketsTemp);

        for(int i=0; i<this.newSocketsTemp.size(); i++){
            SocketChannel newSocket = this.newSocketsTemp.get(i);

            addSocket(newSocket);
       }

       this.newSocketsTemp.clear();
    }


    public TcpSocket addSocket(String host, int tcpPort) throws IOException {
        return addSocket(new InetSocketAddress(host, tcpPort));
    }

    public TcpSocket addSocket(InetSocketAddress address) throws IOException {
        SocketChannel socketChannel = SocketChannel.open(address);
        return addSocket(socketChannel);
    }

    public TcpSocket addTlsSocket(String host, int tcpPort) throws IOException {
        return addTlsSocket(new InetSocketAddress(host, tcpPort));
    }

    public TcpSocket addTlsSocket(InetSocketAddress address) throws IOException {
        SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        Socket socket = sslSocketFactory.createSocket();
        socket.connect(address);

        return addSocket(socket.getChannel());
    }


    public TcpSocket addSocket(SocketChannel newSocket) throws IOException {

        newSocket.configureBlocking(false);

        //todo pool some of these objects - IapMessageFieldsReader etc.
        TcpSocket tcpSocket     = this.tcpObjectPool.getTCPSocket();
        tcpSocket.socketId      = this.nextSocketId++;
        tcpSocket.socketChannel = newSocket;
        tcpSocket.messageReader = this.messageReaderFactory.createMessageReader();
        tcpSocket.messageReader.init(this.readBytesAllocator, this.readObjectPool);

        //新的socket放入map管理
        this.socketMap.put(tcpSocket.socketId, tcpSocket);

        //向read selector注册
        SelectionKey key = newSocket.register(readSelector, SelectionKey.OP_READ);
        key.attach(tcpSocket);

        tcpSocket.readSelectorSelectionKey = key;
        tcpSocket.isRegisteredWithReadSelector = true;

        if(this.socketManager != null){
            this.socketManager.socketAdded(tcpSocket);
        }

        return tcpSocket;
    }


    public int readNow(BytesBatch msgDest) throws IOException {
        int readReady = this.readSelector.selectNow();
        if(readReady > 0) {
            int ready = getReadReadySockets(this.readySocketsTemp, this.readySocketsTemp.length);
            return read(msgDest, this.readySocketsTemp, ready);
        }
        return 0;
    }

    public int readBlock(BytesBatch msgDest) throws IOException {
        while(msgDest.count == 0){
            int readReady = this.readSelector.select();
            if(readReady > 0) {
                int ready = getReadReadySockets(this.readySocketsTemp, this.readySocketsTemp.length);
                read(msgDest, this.readySocketsTemp, ready);
            }
        }
        return msgDest.count;
    }

    protected int getReadReadySockets(TcpSocket[] readyTcpSocket, int limit) throws IOException {

        int readyIndex = 0;
        Iterator<SelectionKey> iterator = this.readSelector.selectedKeys().iterator();
        while(iterator.hasNext() && readyIndex < limit){
            SelectionKey selectionKey = iterator.next();

            if(selectionKey.channel().isOpen()){
                readyTcpSocket[readyIndex++] = (TcpSocket) selectionKey.attachment();
            }
            iterator.remove();
        }
        return readyIndex;
    }

    protected int read(BytesBatch msgDest, TcpSocket[] readReadySockets, int readReadySocketCount) throws IOException {

        int receivedMessageCount = 0;
        for(int i=0; i<readReadySocketCount; i++){
            TcpSocket tcpSocket = readReadySockets[i];

            receivedMessageCount += tcpSocket.readMessages(this.readBuffer, msgDest);

            if(tcpSocket.endOfStreamReached || tcpSocket.state != TcpSocket.STATE_OPEN){
                tcpSocket.readSelectorSelectionKey.cancel();
                tcpSocket.isRegisteredWithReadSelector = false;

                System.out.println("closing socket soon (read)");

                this.socketsToBeClosed.add(tcpSocket);
            }
        }

        return receivedMessageCount;
    }




    /*
     *  Write methods below
     */

    public void writeNow() throws IOException {
        // Cancel all sockets which have no more data to write.
        cancelEmptySockets();

        int writeReady = this.writeSelector.selectNow();
        if(writeReady > 0){
            write();
        }

    }


    public void writeBlock() throws IOException {
        int registeredSockets = this.writeSelector.keys().size();

        while(registeredSockets > 0){
            // Cancel all sockets which have no more data to write.
            cancelEmptySockets();

            // Select from the Selector.
            int writeReady = this.writeSelector.select();
            if(writeReady > 0){
                write();
            }
            registeredSockets = this.writeSelector.keys().size();
        }

    }


    private void write() throws IOException {
        Set<SelectionKey> selectionKeys = this.writeSelector.selectedKeys();
        Iterator<SelectionKey> keyIterator   = selectionKeys.iterator();

        while(keyIterator.hasNext()){
            SelectionKey key = keyIterator.next();

            TcpSocket socket = (TcpSocket) key.attachment();

            socket.writeQueued(this.writeByteBuffer);

            if(socket.isEmpty()){
                this.nonEmptyToEmptySockets.add(socket);
                //this.emptyToNonEmptySockets.remove(socket); //necessary?
            }
            if(socket.state != TcpSocket.STATE_OPEN){
                key.cancel();
                socket.isRegisteredWithWriteSelector = false;
                System.out.println("closing socket soon (write)");

                this.socketsToBeClosed.add(socket);
            }

            keyIterator.remove();
        }

        selectionKeys.clear();
    }



    private void cancelEmptySockets() {
        /*
        if(this.nonEmptyToEmptySockets.size() > 0){
            System.out.println("Canceling socket selector registrations: " + this.nonEmptyToEmptySockets.size());
        };
        */

        //todo could this be optimized if a List was used instead of a Set ?
        if(nonEmptyToEmptySockets.size() == 0) return;


        for(int i=0, n=nonEmptyToEmptySockets.size(); i<n; i++){
            TcpSocket tcpSocket = nonEmptyToEmptySockets.get(i);
            if(tcpSocket.isEmpty()){
                SelectionKey key = tcpSocket.socketChannel.keyFor(this.writeSelector);
                if(key != null){
                    key.cancel();  //todo how can key be null?
                }
                tcpSocket.isRegisteredWithWriteSelector = false;
            }
        }

        nonEmptyToEmptySockets.clear();
    }



    public IapMessage getWriteMemoryBlock() {
        return (IapMessage) this.writeObjectPool.instance();
    }

    public IapMessage allocateWriteMemoryBlock(int lengthToAllocate) {
        IapMessage iapMessage = getWriteMemoryBlock();
        iapMessage.allocate(lengthToAllocate);
        return iapMessage;
    }

    public TcpSocket getTCPSocket(long socketId) {
        return this.socketMap.get(socketId);
    }

    public void writeNowOrEnqueue(IapMessage tcpMessage) throws IOException {
        writeNowOrEnqueue(tcpMessage.getSourceTcpSocket(), tcpMessage);
    }

    public void writeNowOrEnqueue(TcpSocket tcpSocket, IapMessage message) throws IOException {
        if(tcpSocket.isEmpty()){
            //attempt to write message immediately instead of first queueing up the message.
            tcpSocket.write(this.writeByteBuffer, message);

            if(message.readIndex == message.writeIndex){ //if full message written
                message.free();
            } else {  //else queue remainder of message.
                if(!tcpSocket.isRegisteredWithWriteSelector){
                    tcpSocket.socketChannel.register(this.writeSelector, SelectionKey.OP_WRITE, tcpSocket);
                    tcpSocket.isRegisteredWithWriteSelector = true;
                }

                tcpSocket.enqueue(message);
            }

        } else {
            tcpSocket.enqueue(message);
        }

    }


    public List<TcpSocket> getSocketsToBeClosed() {
        return socketsToBeClosed;
    }

    public void cleanupSockets() {
        for(int i=0, n=this.socketsToBeClosed.size(); i < n; i++){
            TcpSocket tcpSocket = this.socketsToBeClosed.get(i);

            try {
                tcpSocket.closeAndFree();
                if(this.socketManager != null){
                    this.socketManager.socketClosed(tcpSocket);
                }
            } catch (IOException e) {
                System.out.println("Error closing TcpSocket:");
                e.printStackTrace();
            }
        }
        this.socketsToBeClosed.clear();

    }

}
