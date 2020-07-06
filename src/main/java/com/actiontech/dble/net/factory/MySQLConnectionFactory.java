package com.actiontech.dble.net.factory;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.impl.nio.NIOConnector;
import com.actiontech.dble.net.impl.nio.NIOReactor;
import com.actiontech.dble.net.impl.nio.NIOSocketWR;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLConnectionFactory extends PooledConnectionFactory {


    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, ResponseHandler handler, String schema) throws IOException {
        NetworkChannel channel = SocketChannel.open();
        ((SocketChannel) channel).configureBlocking(false);
        NIOSocketWR socketWR = new NIOSocketWR();
        BackendConnection connection = new BackendConnection(channel, socketWR, instance, handler);
        socketWR.initFromConnection(connection);
        IOProcessor processor = DbleServer.getInstance().nextBackendProcessor();
        connection.setProcessor(processor);
        ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(connection);
        return connection;
    }

    @Override
    public PooledConnection make(ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) throws IOException {
        NetworkChannel channel = SocketChannel.open();
        ((SocketChannel) channel).configureBlocking(false);
        NIOSocketWR socketWR = new NIOSocketWR();
        BackendConnection connection = new BackendConnection(channel, socketWR, instance, listener, schema);
        socketWR.initFromConnection(connection);
        IOProcessor processor = DbleServer.getInstance().nextBackendProcessor();
        connection.setProcessor(processor);
        ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(connection);
        return connection;
    }
}
