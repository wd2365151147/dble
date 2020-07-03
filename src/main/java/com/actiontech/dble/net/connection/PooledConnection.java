package com.actiontech.dble.net.connection;


import com.actiontech.dble.backend.pool.ConnectionPool;
import com.actiontech.dble.net.SocketWR;

import java.nio.channels.NetworkChannel;
import java.util.Comparator;

public abstract class PooledConnection extends AbstractConnection {

    protected volatile long lastTime;
    private volatile long poolDestroyedTime;
    private volatile String schema;
    private volatile ConnectionPool poolRelated;

    public static final int STATE_REMOVED = -4;
    public static final int STATE_HEARTBEAT = -3;
    public static final int STATE_RESERVED = -2;
    public static final int STATE_IN_USE = -1;
    public static final int INITIAL = 0;
    public static final int STATE_NOT_IN_USE = 1;

    public static final Comparator<PooledConnection> LAST_ACCESS_COMPARABLE;

    static {
        LAST_ACCESS_COMPARABLE = new Comparator<PooledConnection>() {
            @Override
            public int compare(final PooledConnection entryOne, final PooledConnection entryTwo) {
                return Long.compare(entryOne.lastTime, entryTwo.lastTime);
            }
        };
    }

    public PooledConnection(NetworkChannel channel, SocketWR socketWR) {
        super(channel, socketWR);
    }

    public abstract boolean compareAndSet(int expect, int update);

    public abstract void lazySet(int update);

    public abstract int getState();

    public abstract void release();

    public abstract void asynchronousTest();

    public abstract void synchronousTest();

    public abstract void closePooldestroyed(String reason);

    public long getLastTime() {
        return lastTime;
    }


    public long getPoolDestroyedTime() {
        return poolDestroyedTime;
    }

    public void setPoolDestroyedTime(long poolDestroyedTime) {
        this.poolDestroyedTime = poolDestroyedTime;
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public ConnectionPool getPoolRelated() {
        return poolRelated;
    }

    public void setPoolRelated(ConnectionPool poolRelated) {
        this.poolRelated = poolRelated;
    }

    public boolean isFromSlaveDB() {
        return poolRelated.isFromSlave();
    }

}
