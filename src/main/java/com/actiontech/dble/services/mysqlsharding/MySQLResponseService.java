package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.backend.mysql.xa.TxState;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.handler.BackEndCleaner;
import com.actiontech.dble.net.handler.BackEndRecycleRunnable;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.CommandPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.PingPacket;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2020/6/29.
 */
public class MySQLResponseService extends MySQLBasedService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLResponseService.class);
    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;


    private ResponseHandler responseHandler;

    protected final AtomicBoolean isHandling = new AtomicBoolean(false);

    private volatile int resultStatus;

    private volatile boolean isExecuting = false;

    private volatile long lastTime;

    private volatile Object attachment;

    private volatile NonBlockingSession session;


    private final MySQLConnectionStatus status = new MySQLConnectionStatus();

    private volatile String schema = null;
    private volatile String oldSchema;
    private volatile boolean metaDataSynced = true;
    protected volatile Map<String, String> usrVariables;
    protected volatile Map<String, String> sysVariables;
    private final AtomicBoolean logResponse = new AtomicBoolean(false);
    private volatile boolean complexQuery;
    private volatile boolean isDDL = false;
    private volatile boolean testing = false;
    private volatile StatusSync statusSync;
    private volatile boolean autocommit;
    private volatile int txIsolation;
    private volatile boolean isRowDataFlowing = false;
    private volatile BackEndCleaner recycler = null;
    private volatile TxState xaStatus = TxState.TX_INITIALIZE_STATE;

    public MySQLResponseService(AbstractConnection connection) {
        super(connection);
    }

    @Override
    public void handleData(ServiceTask task) {
        handleInnerData(task.getOrgData());
    }

    @Override
    protected void handleInnerData(byte[] data) {
        //todo finish this
    }

    protected void TaskToTotalQueue(ServiceTask task) {
        handleQueue(DbleServer.getInstance().getBackendBusinessExecutor());
    }

    protected void handleQueue(final Executor executor) {
        if (isHandling.compareAndSet(false, true)) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        handleInnerData();
                    } catch (Exception e) {
                        handleDataError(e);
                    } finally {
                        isHandling.set(false);
                        if (taskQueue.size() > 0) {
                            handleQueue(executor);
                        }
                    }
                }
            });
        }
    }

    protected void handleDataError(Exception e) {
        LOGGER.info(this.toString() + " handle data error:", e);
        while (taskQueue.size() > 0) {
            taskQueue.clear();
            // clear all data from the client
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        }
        resultStatus = RESULT_STATUS_INIT;
        connection.close("handle data error:" + e.getMessage());
    }


    private void handleInnerData() {
        ServiceTask task;

        //threadUsageStat start
        String threadName = null;
        ThreadWorkUsage workUsage = null;
        long workStart = 0;
        if (SystemConfig.getInstance().getUseThreadUsageStat() == 1) {
            threadName = Thread.currentThread().getName();
            workUsage = DbleServer.getInstance().getThreadUsedMap().get(threadName);
            if (threadName.startsWith("backend")) {
                if (workUsage == null) {
                    workUsage = new ThreadWorkUsage();
                    DbleServer.getInstance().getThreadUsedMap().put(threadName, workUsage);
                }
            }

            workStart = System.nanoTime();
        }
        //handleData
        while ((task = taskQueue.poll()) != null) {
            handleData(task);
        }
        //threadUsageStat end
        if (workUsage != null && threadName.startsWith("backend")) {
            workUsage.setCurrentSecondUsed(workUsage.getCurrentSecondUsed() + System.nanoTime() - workStart);
        }
    }

    public void ping() {
        this.write(PingPacket.PING);
    }

    public void execCmd(String cmd) {
        this.sendQueryCmd(cmd, this.getConnection().getCharsetName());
    }

    public void sendQueryCmd(String query, CharsetNames clientCharset) {
        CommandPacket packet = new CommandPacket();
        packet.setPacketId(0);
        packet.setCommand(MySQLPacket.COM_QUERY);
        try {
            packet.setArg(query.getBytes(CharsetUtil.getJavaCharset(clientCharset.getClient())));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        isExecuting = true;
        lastTime = TimeUtil.currentTimeMillis();
        int size = packet.calcPacketSize();
        if (size >= MySQLPacket.MAX_PACKET_SIZE) {
            packet.writeBigPackage(this, size);
        } else {
            packet.write(this);
        }
    }


    public String getConnXID(String sessionXaId, long multiplexNum) {
        if (sessionXaId == null)
            return null;
        else {
            String strMultiplexNum = multiplexNum == 0 ? "" : "." + multiplexNum;
            return sessionXaId.substring(0, sessionXaId.length() - 1) + "." + this.schema + strMultiplexNum + "'";
        }
    }


    public boolean syncAndExecute() {
        StatusSync sync = this.statusSync;
        if (sync == null) {
            isExecuting = false;
            return true;
        } else {
            boolean executed = sync.synAndExecuted(this);
            if (executed) {
                isExecuting = false;
                statusSync = null;
            }
            return executed;
        }

    }


    public void release() {

        if (!metaDataSynced) { // indicate connection not normal finished
            LOGGER.info("can't sure connection syn result,so close it " + this);
            this.responseHandler = null;
            this.connection.businessClose("syn status unknown ");
            return;
        }

        if (this.usrVariables.size() > 0) {
            this.responseHandler = null;
            this.connection.businessClose("close for clear usrVariables");
            return;
        }
        if (isRowDataFlowing) {
            if (logResponse.compareAndSet(false, true)) {
                session.setBackendResponseEndTime(this);
            }
            DbleServer.getInstance().getComplexQueryExecutor().execute(new BackEndRecycleRunnable(this));
            return;
        }

        complexQuery = false;
        metaDataSynced = true;
        attachment = null;
        statusSync = null;
        isDDL = false;
        testing = false;
        setResponseHandler(null);
        setSession(null);
        logResponse.set(false);
        ((PooledConnection) connection).getPoolRelated().release((PooledConnection) connection);
    }


    public BackendConnection getConnection() {
        return (BackendConnection) connection;
    }

    public void setResponseHandler(ResponseHandler handler) {
        this.responseHandler = handler;
    }


    public Object getAttachment() {
        return attachment;
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }


    public NonBlockingSession getSession() {
        return session;
    }

    public void setSession(NonBlockingSession session) {
        this.session = session;
    }


    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }


    public boolean isRowDataFlowing() {
        return isRowDataFlowing;
    }

    public void setRowDataFlowing(boolean rowDataFlowing) {
        isRowDataFlowing = rowDataFlowing;
    }

    public BackEndCleaner getRecycler() {
        return recycler;
    }

    public AtomicBoolean getLogResponse() {
        return logResponse;
    }

    public boolean isComplexQuery() {
        return complexQuery;
    }

    public void setComplexQuery(boolean complexQuery) {
        this.complexQuery = complexQuery;
    }

    public boolean isDDL() {
        return isDDL;
    }

    public void setDDL(boolean DDL) {
        isDDL = DDL;
    }

    public boolean isTesting() {
        return testing;
    }

    public void setTesting(boolean testing) {
        this.testing = testing;
    }

    public StatusSync getStatusSync() {
        return statusSync;
    }

    public void setStatusSync(StatusSync statusSync) {
        this.statusSync = statusSync;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public void setRecycler(BackEndCleaner recycler) {
        this.recycler = recycler;
    }

    public TxState getXaStatus() {
        return xaStatus;
    }

    public void setXaStatus(TxState xaStatus) {
        this.xaStatus = xaStatus;
    }

    private static class StatusSync {
        private final String schema;
        private final CharsetNames clientCharset;
        private final Integer txtIsolation;
        private final Boolean autocommit;
        private final AtomicInteger synCmdCount;
        private final Map<String, String> usrVariables = new LinkedHashMap<>();
        private final Map<String, String> sysVariables = new LinkedHashMap<>();

        StatusSync(String schema,
                   CharsetNames clientCharset, Integer txtIsolation, Boolean autocommit,
                   int synCount, Map<String, String> usrVariables, Map<String, String> sysVariables, Set<String> toResetSys) {
            super();
            this.schema = schema;
            this.clientCharset = clientCharset;
            this.txtIsolation = txtIsolation;
            this.autocommit = autocommit;
            this.synCmdCount = new AtomicInteger(synCount);
            this.usrVariables.putAll(usrVariables);
            this.sysVariables.putAll(sysVariables);
            for (String sysVariable : toResetSys) {
                this.sysVariables.remove(sysVariable);
            }
        }

        boolean synAndExecuted(MySQLResponseService service) {
            int remains = synCmdCount.decrementAndGet();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("synAndExecuted " + remains + ",conn info:" + service);
            }
            if (remains == 0) { // syn command finished
                this.updateConnectionInfo(service);
                service.metaDataSynced = true;
                return false;
            }
            return remains < 0;
        }

        private void updateConnectionInfo(MySQLResponseService service) {
            if (schema != null) {
                service.schema = schema;
                service.oldSchema = service.schema;
            }
            if (clientCharset != null) {
                service.connection.setCharsetName(clientCharset);
            }
            if (txtIsolation != null) {
                service.txIsolation = txtIsolation;
            }
            if (autocommit != null) {
                service.autocommit = autocommit;
            }
            service.sysVariables = sysVariables;
            service.usrVariables = usrVariables;
        }
    }
}
