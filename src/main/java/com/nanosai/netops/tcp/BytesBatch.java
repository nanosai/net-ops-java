package com.nanosai.netops.tcp;

import com.nanosai.memops.objects.Bytes;

/**
 * Represents a batch of Bytes instances - typically read in from a TcpSocket.
 *
 * todo This class might belong somewhere else - but it's located here to get Net Ops to compile after extraction from Grid Ops
 */
public class BytesBatch {


    public Bytes[] blocks = null;
    public int count = 0;

    public int limit = 64;

    public BytesBatch(Bytes[] bytesArray) {
        this.blocks = bytesArray;
        this.limit  = blocks.length;  // use initial length as limit
    }

    public BytesBatch(Bytes[] bytesArray, int limit) {
        this.blocks = bytesArray;
        this.limit  = limit;
    }

    public BytesBatch(int initialCapacity){
        this(new Bytes[initialCapacity] );
    }

    public BytesBatch(int initialCapacity, int limit){
        this(new Bytes[initialCapacity], limit);
    }


    public void add(Bytes bytes){
        if(this.count == this.blocks.length){
            Bytes[] newBlocks = new Bytes[this.blocks.length + 16];
            System.arraycopy(this.blocks, 0, newBlocks, 0, this.blocks.length);

            this.blocks = newBlocks;
        }

        this.blocks[this.count++] = bytes;
    }

    public void clear() {
        this.count = 0;
    }


}
