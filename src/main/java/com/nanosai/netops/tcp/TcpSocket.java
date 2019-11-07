package com.nanosai.netops.tcp;



import com.nanosai.memops.objects.Bytes;
import com.nanosai.netops.iap.IapMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by jjenkov on 27-10-2015.
 */
public class TcpSocket {

    public static final int STATE_OPEN   = 0;
    public static final int STATE_FAULTY = 1;
    public static final int STATE_CLOSED = 2;

    private TcpSocketPool tcpSocketPool = null;

    public SocketChannel    socketChannel = null;
    public long             socketId = 0;
    public SelectionKey     readSelectorSelectionKey = null;
    public boolean          isRegisteredWithReadSelector = false;


    public IMessageReader   messageReader = null;

    public boolean          isRegisteredWithWriteSelector = false;
    public boolean          endOfStreamReached = false;
    public int              state = 0;

    //public SecureChannelStates secureChannelStates = new SecureChannelStates();

    /*
       WRITING VARIABLES
     */

    private Queue writeQueue   = new Queue(16);


    public TcpSocket(TcpSocketPool tcpSocketPool) {
        this.tcpSocketPool = tcpSocketPool;
    }


    public int readMessages(ByteBuffer tempBuffer, BytesBatch messageDestination) throws IOException {
        if(this.state != STATE_OPEN) {
            return 0; //TcpSocket is in an invalid state - no more messages can be read from it.
        }

        tempBuffer.clear();

        int totalBytesRead = read(tempBuffer);

        int messagesRead = 0;
        if(totalBytesRead > 0){
            tempBuffer.flip();

            messagesRead = this.messageReader.read(tempBuffer, messageDestination);

            if(this.messageReader.state() != 0){
                // continue processing the valid messages received in the for loop below - but by setting state to
                // something other than 0, no more messages can be read from this TCP Socket. It is now invalid and
                // should be closed.
                this.state = this.messageReader.state();
            }

            for(int i=messageDestination.count - messagesRead, n = messageDestination.count; i < n; i++){
                IapMessage tcpMessage = (IapMessage) messageDestination.blocks[i];
                tcpMessage.setTcpSocket(this.socketId, this);
            }
        }

        return messagesRead;
    }

    public int read(ByteBuffer destinationBuffer) throws IOException {
        int bytesRead = 0;

        try{
            bytesRead = doSocketRead(destinationBuffer);
        } catch(IOException e){
            this.endOfStreamReached = true;
            this.state = STATE_FAULTY;
            return -1;
        }

        int totalBytesRead = bytesRead;

        while(bytesRead > 0){
            try{
                bytesRead = doSocketRead(destinationBuffer);
                if(bytesRead > 0){
                    totalBytesRead += bytesRead;
                }
            } catch(IOException e){
                this.endOfStreamReached = true;
                this.state = STATE_FAULTY;
                return -1;
            }
        }

        if(bytesRead == -1){
            this.endOfStreamReached = true;
        }

        return totalBytesRead;
    }


    /**
     * A method which can be overwritten in mock classes - to NOT read from a socketChannel, but from e.g. a
     * predefined byte array.
     *
     * @param destinationBuffer The ByteBuffer to read the bytes into, from the underlying SocketChannel
     * @return The number of bytes read from the underlying SocketChannel.
     * @throws IOException If reading from the underlying SocketChannel fails.
     */
    protected int doSocketRead(ByteBuffer destinationBuffer) throws IOException {
        return this.socketChannel.read(destinationBuffer);
    }

    public void enqueue(Bytes memoryBlock) {
        this.writeQueue.put(memoryBlock);
    }


    public boolean isEmpty() {
        return this.writeQueue.available() == 0;
    }


    public boolean write(ByteBuffer byteBuffer, IapMessage message) throws IOException {
        byteBuffer.clear();

        //todo make some calculations to limit the size of data written to that of the ByteBuffer.capacity()
        byteBuffer.put(message.data, message.readIndex, message.writeIndex - message.readIndex);

        byteBuffer.flip();

        int bytesWrittenNow = 0;

        do {
            try{
                bytesWrittenNow = doSocketWrite(byteBuffer);
            } catch(IOException e){
                this.endOfStreamReached = true;
                this.state = STATE_FAULTY;
                break; //exit do-while loop.
            }
            message.readIndex += bytesWrittenNow;
        } while(bytesWrittenNow > 0 && byteBuffer.hasRemaining());

        return bytesWrittenNow > 0; //can write more? Or was bytesWrittenNow == 0 ?
    }

    /**
     * Write messages queued up to underlying SocketChannel, using ByteBuffer parameter to keep data in
     * during the write process.
     *
     * @param byteBuffer The ByteBuffer that is used to transport the data from queued messages into
     *                   the underlying SocketChannel.
     * @throws IOException If writing to the underlying SocketChannel fails.
     */
    public void writeQueued(ByteBuffer byteBuffer) throws IOException {
        IapMessage messageInProgress = (IapMessage) this.writeQueue.peek();

        boolean canWriteMoreToSocketNow = true;

        while(canWriteMoreToSocketNow && messageInProgress != null){
             canWriteMoreToSocketNow = write(byteBuffer, messageInProgress);

            if(messageInProgress.readIndex == messageInProgress.writeIndex){ //if message fully written to socket...
                this.writeQueue.take();     // remove this message from queue
                messageInProgress.free();   // free the memory allocated to this message

                messageInProgress = (IapMessage) this.writeQueue.peek();  // take next message in queue, if any.
            }
        }
    }



    public int doSocketWrite(ByteBuffer byteBuffer) throws IOException{
        int bytesWritten      = this.socketChannel.write(byteBuffer);
        int totalBytesWritten = bytesWritten;

        while(bytesWritten > 0 && byteBuffer.hasRemaining()){
            bytesWritten = this.socketChannel.write(byteBuffer);
            totalBytesWritten += bytesWritten;
        }

        return totalBytesWritten;
    }


    /**
     * Closes the TcpSocket + underlying SocketChannel, and frees up all queued inbound and outbound
     * messages.
     *
     * @throws IOException If something fails when closing the underlying SocketChannel.
     *
     * todo maybe split into three methods: closeOutputStream() + free() + closeAndFree()
     */
    public void closeAndFree() throws IOException {
        if(this.messageReader != null){
            this.messageReader.dispose();
            this.messageReader = null;
        }

        if(this.writeQueue != null){
            while(this.writeQueue.available() > 0){
                IapMessage queuedOutboundMessage = (IapMessage) writeQueue.take();
                queuedOutboundMessage.free();
            }
        }

        if(this.readSelectorSelectionKey != null){
            this.readSelectorSelectionKey.attach(null);
            if(this.isRegisteredWithReadSelector){
                this.readSelectorSelectionKey.cancel();
            }
            this.readSelectorSelectionKey = null;
        }

        if(this.socketChannel != null){
            this.socketChannel.close();
            this.socketChannel = null;
        }
    }


}
