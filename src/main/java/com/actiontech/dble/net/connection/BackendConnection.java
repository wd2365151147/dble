package com.actiontech.dble.net.connection;


import com.actiontech.dble.net.SocketWR;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;

import java.nio.channels.NetworkChannel;

/**
 * Created by szf on 2020/6/23.
 */
public class BackendConnection extends PooledConnection {

    private long threadId = 0;

    public BackendConnection(NetworkChannel channel, SocketWR socketWR) {
        super(channel, socketWR);
    }


    @Override
    public void businessClose(String reason) {

    }

    @Override
    public void setConnProperties(AuthResultInfo info) {

    }

    @Override
    public void startFlowControl(com.actiontech.dble.backend.BackendConnection bcon) {

    }

    @Override
    public void stopFlowControl() {

    }

    public void onConnectFailed(Throwable e) {

    }

    @Override
    public boolean compareAndSet(int expect, int update) {
        return false;
    }

    @Override
    public void lazySet(int update) {

    }

    @Override
    public int getState() {
        return 0;
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
