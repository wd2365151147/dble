package com.actiontech.dble.net.connection;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AuthResultInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class FrontendConnection extends AbstractConnection {


    private final boolean isManager;

    public FrontendConnection(NetworkChannel channel, SocketWR socketWR, boolean isManager) throws IOException {
        super(channel, socketWR);
        this.isManager = isManager;
        InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        InetSocketAddress remoteAddress = null;
        if (channel instanceof SocketChannel) {
            remoteAddress = (InetSocketAddress) ((SocketChannel) channel).getRemoteAddress();
        } else if (channel instanceof AsynchronousSocketChannel) {
            remoteAddress = (InetSocketAddress) ((AsynchronousSocketChannel) channel).getRemoteAddress();
        } else {
            throw new RuntimeException("FrontendConnection type is" + channel.getClass());
        }
        this.host = remoteAddress.getHostString();
        this.port = localAddress.getPort();
        this.localPort = remoteAddress.getPort();
    }

    @Override
    public void businessClose(String reason) {

    }

    @Override
    public void setConnProperties(AuthResultInfo info) {

    }

    @Override
    public void startFlowControl(BackendConnection bcon) {

    }

    @Override
    public void stopFlowControl() {

    }


    public boolean isManager() {
        return isManager;
    }
}
