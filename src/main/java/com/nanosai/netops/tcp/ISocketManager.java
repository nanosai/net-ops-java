package com.nanosai.netops.tcp;

/**
 * An ISocketManager helps manage the sockets (connections) managed by a TcpMessagePort.
 */
public interface ISocketManager {


    /**
     * Called when the ISocketManager is first set on TcpMessagePort.
     *
     * @param tcpMessagePort The TcpMessagePort the ISocketManager is added to.
     */
    public void init(TcpMessagePort tcpMessagePort);

    /**
     * Called when a new TcpSocket is added to the TcpSocketManager
     *
     * @param tcpSocket The newly added socket.
     */
    public void socketAdded(TcpSocket tcpSocket);


    /**
     * Called after a TcpSocket is closed by the TcpMessagePort
     * @param tcpSocket The tcpSocket that was just closed the the TcpMessagePort
     */
    public void socketClosed(TcpSocket tcpSocket);


}
