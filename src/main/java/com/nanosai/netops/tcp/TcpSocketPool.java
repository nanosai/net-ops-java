package com.nanosai.netops.tcp;

/**
 * Created by jjenkov on 01-02-2016.
 */
public class TcpSocketPool {

    private TcpSocket[] pooledSockets = null;
    private int pooledSocketCount = 0;

    public TcpSocketPool(int maxPooledSockets) {
        this.pooledSockets  = new TcpSocket[maxPooledSockets];
    }


    public TcpSocket getTCPSocket(){
        if(this.pooledSocketCount > 0){
            this.pooledSocketCount--;
            return this.pooledSockets[this.pooledSocketCount];
        }
        return new TcpSocket(this);
    }

    public int freePooledTCPSocketCount() {
        return this.pooledSocketCount;
    }

    public void free(TcpSocket socket){
        //pool message if space
        if(this.pooledSocketCount < this.pooledSockets.length){
            this.pooledSockets[this.pooledSocketCount] = socket;
            this.pooledSocketCount++;
        }
    }


}
