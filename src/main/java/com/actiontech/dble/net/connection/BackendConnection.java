package com.actiontech.dble.net.connection;


import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.backend.pool.ReadTimeStatusInstance;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.WriteOutTask;
import com.actiontech.dble.net.mysql.QuitPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.mysqlauthenticate.MySQLBackAuthService;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.util.TimeUtil;

import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class BackendConnection extends PooledConnection {

    private long threadId = 0;

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, ResponseHandler handler) {
        super(channel, socketWR);
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
        this.lastTime = TimeUtil.currentTimeMillis();
        this.setService(new MySQLBackAuthService(this, config.getUser(), null, config.getPassword(), null, handler));

        /*this.autocommitSynced = instance.isAutocommitSynced();
        boolean sysAutocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.autocommit = sysAutocommit == autocommitSynced; // T + T-> T, T + F-> F, F +T ->F, F + F->T
        this.fromSlaveDB = fromSlaveDB;
        this.isolationSynced = isolationSynced;
        if (isolationSynced) {
            this.txIsolation = SystemConfig.getInstance().getTxIsolation();
        } else {
            *//* if the txIsolation in bootstrap.cnf is different from the isolation level in MySQL node,
             * it need to sync the status firstly for new idle connection*//*
            this.txIsolation = -1;
        }
        this.complexQuery = false;
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
        this.user = config.getUser();
        this.password = config.getPassword();*/

    }

    public BackendConnection(NetworkChannel channel, SocketWR socketWR, ReadTimeStatusInstance instance, PooledConnectionListener listener, String schema) {
        super(channel, socketWR);
        DbInstanceConfig config = instance.getConfig();
        this.host = config.getIp();
        this.port = config.getPort();
        this.lastTime = TimeUtil.currentTimeMillis();
        this.setService(new MySQLBackAuthService(this, config.getUser(), schema, config.getPassword(), listener, null));
    }


    @Override
    public void businessClose(String reason) {
        this.getBackendService().setResponseHandler(null);
        this.close(reason);
    }


    @Override
    public void setConnProperties(AuthResultInfo info) {

    }


    @Override
    public void stopFlowControl() {

    }

    public void onConnectFailed(Throwable e) {

    }

    @Override
    public void startFlowControl(BackendConnection bcon) {

    }

    @Override
    public void release() {
        getBackendService().release();
    }

    @Override
    public void asynchronousTest() {

    }

    @Override
    public void synchronousTest() {

    }

    @Override
    public void closePooldestroyed(String reason) {
        this.getBackendService().getConnection().close(reason);
        this.close(reason);
    }

    @Override
    public synchronized void close(final String reason) {
        boolean isAuthed = !(this.getService() instanceof AuthService);
        if (!isClosed) {
            if (isAuthed && channel.isOpen() && closeReason != null) {
                try {
                    GracefulClose(reason);
                } catch (Throwable e) {
                    LOGGER.info("error when try to quit the connection ,drop the error and close it anyway", e);
                    super.close(reason);
                    this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
                }
            } else {
                super.close(reason);
                if (isAuthed) {
                    this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
                }
            }
            if (isAuthed) {
                this.getBackendService().backendSpecialCleanUp();
            }
        } else {
            this.cleanup();
            if (isAuthed) {
                this.getBackendService().onConnectionClose(reason == null ? closeReason : reason);
            }
        }
    }


    private void GracefulClose(String reason) {
        this.closeReason = reason;
        writeClose(writeToBuffer(QuitPacket.QUIT, allocate()));
    }

    public void writeClose(ByteBuffer buffer) {
        writeQueue.offer(new WriteOutTask(buffer, true));
        try {
            this.socketWR.doNextWriteCheck();
        } catch (Exception e) {
            LOGGER.info("write err:", e);
            this.close("write err:" + e);
        }
    }


    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public MySQLResponseService getBackendService() {
        return (MySQLResponseService) getService();
    }
}