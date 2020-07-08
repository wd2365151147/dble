/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.*;
import com.actiontech.dble.backend.mysql.nio.handler.builder.BaseHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.builder.HandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.DMLResponseHandler;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.OutputHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.ImplicitCommitHandler;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.TransactionHandlerManager;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.savepoint.SavePointHandler;
import com.actiontech.dble.backend.mysql.store.memalloc.MemSizeController;
import com.actiontech.dble.btrace.provider.ComplexQueryProvider;
import com.actiontech.dble.btrace.provider.CostTimeProvider;
import com.actiontech.dble.cluster.values.DDLTraceInfo;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.net.handler.BackEndDataCleaner;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.mysql.StatusFlags;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.trace.TraceRecord;
import com.actiontech.dble.server.trace.TraceResult;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;
import com.actiontech.dble.singleton.DDLTraceManager;
import com.actiontech.dble.singleton.PauseShardingNodeManager;
import com.actiontech.dble.singleton.ProxyMeta;
import com.actiontech.dble.statistic.stat.QueryTimeCost;
import com.actiontech.dble.statistic.stat.QueryTimeCostContainer;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.meta.PauseEndThreadPool.CONTINUE_TYPE_MULTIPLE;
import static com.actiontech.dble.meta.PauseEndThreadPool.CONTINUE_TYPE_SINGLE;
import static com.actiontech.dble.server.parser.ServerParse.DDL;

/**
 * @author mycat
 */
public class NonBlockingSession implements Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);

    private long queryStartTime = 0;
    private final MySQLShardingService service;
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;
    private final AtomicLong queriesCounter = new AtomicLong(0);
    private final AtomicLong transactionsCounter = new AtomicLong(0);

    private SavePointHandler savePointHandler;
    private TransactionHandlerManager transactionManager;

    private boolean prepared;
    private volatile boolean needWaitFinished = false;

    // kill query
    private volatile boolean killed = false;
    private volatile boolean discard = false;

    private OutputHandler outputHandler;

    // the memory controller for join,orderby,other in this session
    private MemSizeController joinBufferMC;
    private MemSizeController orderBufferMC;
    private MemSizeController otherBufferMC;
    private QueryTimeCost queryTimeCost;
    private CostTimeProvider provider;
    private ComplexQueryProvider xprovider;
    private volatile boolean timeCost = false;
    private AtomicBoolean firstBackConRes = new AtomicBoolean(false);


    private AtomicBoolean isMultiStatement = new AtomicBoolean(false);
    private volatile String remingSql = null;
    private volatile boolean traceEnable = false;
    private volatile TraceResult traceResult = new TraceResult();
    private volatile RouteResultset complexRrs = null;
    private volatile SessionStage sessionStage = SessionStage.Init;

    private volatile long rowCountCurrentSQL = -1;
    private volatile long rowCountLastSQL = 0;

    private final HashSet<BackendConnection> flowControlledBackendConnections = new HashSet<>();

    public NonBlockingSession(MySQLShardingService service) {
        this.service = service;
        this.target = new ConcurrentHashMap<>(2, 1f);
        this.joinBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getJoinMemSize());
        this.orderBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getOrderMemSize());
        this.otherBufferMC = new MemSizeController(1024L * 1024L * SystemConfig.getInstance().getOtherMemSize());
        this.transactionManager = new TransactionHandlerManager(this);
        if (SystemConfig.getInstance().getUseSerializableMode() == 1) {
            transactionManager.setXaTxEnabled(true, service);
        }
    }

    public void setOutputHandler(OutputHandler outputHandler) {
        this.outputHandler = outputHandler;
    }

    void setRequestTime() {
        sessionStage = SessionStage.Read_SQL;
        long requestTime = 0;

        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            requestTime = System.nanoTime();
            traceResult.setVeryStartPrepare(requestTime);
            traceResult.setRequestStartPrepare(new TraceRecord(requestTime));
        }
        if (SystemConfig.getInstance().getUseCostTimeStat() == 0) {
            return;
        }
        timeCost = false;
        if (ThreadLocalRandom.current().nextInt(100) >= SystemConfig.getInstance().getCostSamplePercent()) {
            return;
        }
        timeCost = true;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear");
        }
        queryTimeCost = new QueryTimeCost();
        provider = new CostTimeProvider();
        xprovider = new ComplexQueryProvider();
        provider.beginRequest(service.getConnection().getId());
        if (requestTime == 0) {
            requestTime = System.nanoTime();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("frontend connection setRequestTime:" + requestTime);
        }
        queryTimeCost.setRequestTime(requestTime);
    }

    public void startProcess() {
        sessionStage = SessionStage.Parse_SQL;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setParseStartPrepare(new TraceRecord(System.nanoTime()));
        }
        if (!timeCost) {
            return;
        }
        provider.startProcess(service.getConnection().getId());
    }

    public void endParse() {
        sessionStage = SessionStage.Route_Calculation;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.ready();
            traceResult.setRouteStart(new TraceRecord(System.nanoTime()));
        }
        if (!timeCost) {
            return;
        }
        provider.endParse(service.getConnection().getId());
    }


    public void endRoute(RouteResultset rrs) {
        sessionStage = SessionStage.Prepare_to_Push;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setPreExecuteStart(new TraceRecord(System.nanoTime()));
        }
        if (!timeCost) {
            return;
        }
        provider.endRoute(service.getConnection().getId());
        queryTimeCost.setCount(rrs.getNodes() == null ? 0 : rrs.getNodes().length);
    }

    public void endComplexRoute() {
        if (!timeCost) {
            return;
        }
        xprovider.endRoute(service.getConnection().getId());
    }

    public void endComplexExecute() {
        if (!timeCost) {
            return;
        }
        xprovider.endComplexExecute(service.getConnection().getId());
    }

    public void readyToDeliver() {
        if (!timeCost) {
            return;
        }
        provider.readyToDeliver(service.getConnection().getId());
    }

    public void setPreExecuteEnd(boolean isComplexQuery) {
        sessionStage = SessionStage.Execute_SQL;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setComplexQuery(isComplexQuery);
            traceResult.setPreExecuteEnd(new TraceRecord(System.nanoTime()));
            traceResult.clearConnReceivedMap();
            traceResult.clearConnFlagMap();
        }
    }

    public long getRowCount() {
        return rowCountLastSQL;
    }

    public void setSubQuery() {
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setSubQuery(true);
        }
    }

    public void setBackendRequestTime(long backendID) {
        if (!timeCost) {
            return;
        }
        QueryTimeCost backendCost = new QueryTimeCost();
        long requestTime = System.nanoTime();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("backend connection[" + backendID + "] setRequestTime:" + requestTime);
        }
        backendCost.setRequestTime(requestTime);
        queryTimeCost.getBackEndTimeCosts().put(backendID, backendCost);


    }

    public void setBackendResponseTime(MySQLResponseService service) {
        sessionStage = SessionStage.Fetching_Result;
        long responseTime = 0;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            if (traceResult.addToConnFlagMap(service.getConnection().getId() + ":" + node.getStatementHash()) == null) {
                ResponseHandler responseHandler = service.getResponseHandler();
                responseTime = System.nanoTime();
                TraceRecord record = new TraceRecord(responseTime, node.getName(), node.getStatement());
                Map<MySQLResponseService, TraceRecord> connMap = new ConcurrentHashMap<>();
                connMap.put(service, record);
                traceResult.addToConnReceivedMap(responseHandler, connMap);
            }
        }
        if (!timeCost) {
            return;
        }
        QueryTimeCost backCost = queryTimeCost.getBackEndTimeCosts().get(service.getConnection().getId());
        if (responseTime == 0) {
            responseTime = System.nanoTime();
        }
        if (backCost != null && backCost.getResponseTime().compareAndSet(0, responseTime)) {
            if (queryTimeCost.getFirstBackConRes().compareAndSet(false, true)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("backend connection[" + service.getConnection().getId() + "] setResponseTime:" + responseTime);
                }
                provider.resFromBack(this.service.getConnection().getId());
                firstBackConRes.set(false);
            }
            long index = queryTimeCost.getBackendReserveCount().decrementAndGet();
            if (index >= 0 && ((index % 10 == 0) || index < 10)) {
                provider.resLastBack(this.service.getConnection().getId(), queryTimeCost.getBackendSize() - index);
            }
        }
    }

    public void startExecuteBackend(long backendID) {
        if (!timeCost) {
            return;
        }
        if (firstBackConRes.compareAndSet(false, true)) {
            provider.startExecuteBackend(service.getConnection().getId());
        }
        long index = queryTimeCost.getBackendExecuteCount().decrementAndGet();
        if (index >= 0 && ((index % 10 == 0) || index < 10)) {
            provider.execLastBack(service.getConnection().getId(), queryTimeCost.getBackendSize() - index);
        }
    }

    public void allBackendConnReceive() {
        if (!timeCost) {
            return;
        }
        provider.allBackendConnReceive(service.getConnection().getId());
    }

    public void setResponseTime(boolean isSuccess) {
        sessionStage = SessionStage.Finished;
        long responseTime = 0;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            responseTime = System.nanoTime();
            traceResult.setVeryEnd(responseTime);
            if (isSuccess) {
                SlowQueryLog.getInstance().putSlowQueryLog(this.service, (TraceResult) traceResult.clone());
            }
        }
        if (!timeCost) {
            return;
        }
        if (responseTime == 0) {
            responseTime = System.nanoTime();
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("setResponseTime:" + responseTime);
        }
        queryTimeCost.getResponseTime().set(responseTime);
        provider.beginResponse(service.getConnection().getId());
        QueryTimeCostContainer.getInstance().add(queryTimeCost);
    }

    public void setStageFinished() {
        sessionStage = SessionStage.Finished;
    }

    public void setBackendResponseEndTime(MySQLResponseService service) {
        sessionStage = SessionStage.First_Node_Fetched_Result;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
            ResponseHandler responseHandler = service.getResponseHandler();
            TraceRecord record = new TraceRecord(System.nanoTime(), node.getName(), node.getStatement());
            Map<MySQLResponseService, TraceRecord> connMap = new ConcurrentHashMap<>();
            connMap.put(service, record);
            traceResult.addToConnFinishedMap(responseHandler, connMap);
        }

        if (!timeCost) {
            return;
        }
        if (queryTimeCost.getFirstBackConEof().compareAndSet(false, true)) {
            xprovider.firstComplexEof(this.service.getConnection().getId());
        }
    }

    public void setBeginCommitTime() {
        sessionStage = SessionStage.Distributed_Transaction_Commit;
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setAdtCommitBegin(new TraceRecord(System.nanoTime()));
        }
    }

    public void setFinishedCommitTime() {
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setAdtCommitEnd(new TraceRecord(System.nanoTime()));
        }
    }

    public void setHandlerStart(DMLResponseHandler handler) {
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.addToRecordStartMap(handler, new TraceRecord(System.nanoTime()));
        }
    }

    public void setHandlerEnd(DMLResponseHandler handler) {
        if (handler.getNextHandler() != null) {
            DMLResponseHandler next = handler.getNextHandler();
            sessionStage = SessionStage.changeFromHandlerType(next.type());
        }
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.addToRecordEndMap(handler, new TraceRecord(System.nanoTime()));
        }
    }

    public List<String[]> genTraceResult() {
        if (traceEnable) {
            return traceResult.genTraceResult();
        } else {
            return null;
        }
    }

    public List<String[]> genRunningSQLStage() {
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            TraceResult tmpResult = (TraceResult) traceResult.clone();
            return tmpResult.genRunningSQLStage();
        } else {
            return null;
        }
    }

    @Override
    public FrontendConnection getFrontConnection() {
        return (FrontendConnection) service.getConnection();
    }

    @Override
    public int getTargetCount() {
        return target.size();
    }

    public Set<RouteResultsetNode> getTargetKeys() {
        return target.keySet();
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
        return this.target;
    }

    public boolean isNeedWaitFinished() {
        return needWaitFinished;
    }

    public SessionStage getSessionStage() {
        return sessionStage;
    }

    @Override
    public void execute(RouteResultset rrs) {
        if (killed) {
            service.writeErrMessage(ErrorCode.ER_QUERY_INTERRUPTED, "The query is interrupted.");
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            LOGGER.debug(s.append(service).append(rrs).toString() + " rrs ");
        }

        if (PauseShardingNodeManager.getInstance().getIsPausing().get() &&
                !PauseShardingNodeManager.getInstance().checkTarget(target) &&
                PauseShardingNodeManager.getInstance().checkRRS(rrs)) {
            if (PauseShardingNodeManager.getInstance().waitForResume(rrs, service, CONTINUE_TYPE_SINGLE)) {
                return;
            }
        }

        // complex query
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
            if (rrs.isNeedOptimizer()) {
                try {
                    this.complexRrs = rrs;
                    executeMultiSelect(rrs);
                } catch (MySQLOutPutException e) {
                    service.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
                }
            } else {
                service.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                        "No shardingNode found ,please check tables defined in schema:" + service.getSchema());
            }
            return;
        }

        setRouteResultToTrace(nodes);
        if (rrs.getSqlType() == DDL) {
            // ddl
            executeDDL(rrs);
        } else {
            // dml or simple select
            executeOther(rrs);
        }
    }

    public void setRouteResultToTrace(RouteResultsetNode[] nodes) {
        if (SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setShardingNodes(nodes);
        }
    }

    private void executeDDL(RouteResultset rrs) {
        ExecutableHandler executableHandler;
        try {
            DDLTraceManager.getInstance().startDDL(service);
            // not hint and not online ddl
            if (rrs.getSchema() != null && !rrs.isOnline()) {
                addTableMetaLock(rrs);
                DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.LOCK_END, service);
            }

            if (rrs.getNodes().length == 1) {
                executableHandler = new SingleNodeDDLHandler(rrs, this);
            } else {
                /*
                 * here, just a try! The sync is the superfluous, because there are heartbeats  at every backend node.
                 * We don't do 2pc or 3pc. Because mysql(that is, resource manager) don't support that for ddl statements.
                 */
                checkBackupStatus();
                executableHandler = new MultiNodeDdlPrepareHandler(rrs, this);
            }

            setTraceSimpleHandler((ResponseHandler) executableHandler);
            executableHandler.execute();
            discard = true;
        } catch (Exception e) {
            LOGGER.info(String.valueOf(service) + rrs, e);
            handleSpecial(rrs, false, null);
            service.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
        }
    }

    private void executeOther(RouteResultset rrs) {
        ExecutableHandler executableHandler;
        if (rrs.getNodes().length == 1) {
            executableHandler = new SingleNodeHandler(rrs, this);
        } else if (ServerParse.SELECT == rrs.getSqlType() && rrs.getGroupByCols() != null) {
            executableHandler = new MultiNodeSelectHandler(rrs, this);
        } else {
            executableHandler = new MultiNodeQueryHandler(rrs, this);
        }

        setTraceSimpleHandler((ResponseHandler) executableHandler);
        setPreExecuteEnd(false);
        readyToDeliver();

        try {
            executableHandler.execute();
            discard = true;
        } catch (Exception e) {
            LOGGER.info(String.valueOf(service) + rrs, e);
            executableHandler.clearAfterFailExecute();
            setResponseTime(false);
            service.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
        }
    }

    private void executeMultiResultSet(PlanNode node) {
        init();
        HandlerBuilder builder = new HandlerBuilder(node, this);
        try {
            BaseHandlerBuilder baseBuilder = builder.build();
            if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
                traceResult.setBuilder(baseBuilder);
            }
            discard = true;
        } catch (SQLSyntaxErrorException e) {
            LOGGER.info(service + " execute plan is : " + node, e);
            service.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
        } catch (NoSuchElementException e) {
            LOGGER.info(service + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            service.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
        } catch (MySQLOutPutException e) {
            LOGGER.info(service + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            service.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            LOGGER.info(service + " execute plan is : " + node, e);
            this.closeAndClearResources("Exception");
            service.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
        }
    }

    public void executeMultiSelect(RouteResultset rrs) {
        SQLSelectStatement ast = (SQLSelectStatement) rrs.getSqlStatement();
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(service.getSchema(), service.getCharset().getResultsIndex(), ProxyMeta.getInstance().getTmManager(), false, service.getUsrVariables());
        visitor.visit(ast);
        PlanNode node = visitor.getTableNode();
        if (node.isCorrelatedSubQuery()) {
            throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Correlated Sub Queries is not supported ");
        }
        node.setSql(rrs.getStatement());
        node.setUpFields();
        PlanUtil.checkTablesPrivilege(service, node, ast);
        node = MyOptimizer.optimize(node);

        if (PauseShardingNodeManager.getInstance().getIsPausing().get() &&
                !PauseShardingNodeManager.getInstance().checkTarget(target) &&
                PauseShardingNodeManager.getInstance().checkReferredTableNodes(node.getReferedTableNodes())) {
            if (PauseShardingNodeManager.getInstance().waitForResume(rrs, this.service, CONTINUE_TYPE_MULTIPLE)) {
                return;
            }
        }
        setPreExecuteEnd(true);
        if (PlanUtil.containsSubQuery(node)) {
            setSubQuery();
            final PlanNode finalNode = node;
            //sub Query build will be blocked, so use ComplexQueryExecutor
            DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
                executeMultiResultSet(finalNode);
            });
        } else {
            if (!visitor.isContainSchema()) {
                node.setAst(ast);
            }
            executeMultiResultSet(node);
        }
    }

    private void addTableMetaLock(RouteResultset rrs) throws SQLNonTransientException {
        String schema = rrs.getSchema();
        String table = rrs.getTable();
        try {
            //lock self meta
            ProxyMeta.getInstance().getTmManager().addMetaLock(schema, table, rrs.getSrcStatement());
            ProxyMeta.getInstance().getTmManager().notifyClusterDDL(schema, table, rrs.getStatement());
        } catch (Exception e) {
            ProxyMeta.getInstance().getTmManager().removeMetaLock(schema, table);
            throw new SQLNonTransientException(e.toString() + ",sql:" + rrs.getStatement());
        }
    }

    private void init() {
        this.outputHandler = null;
    }

    public void onQueryError(byte[] message) {
        if (outputHandler != null) {
            outputHandler.backendConnError(message);
        } else {
            String error = new String(message);
            this.closeAndClearResources(error);
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, error);
        }
    }

    public TransactionHandlerManager getTransactionManager() {
        return transactionManager;
    }

    public void commit() {
        checkBackupStatus();
        transactionManager.commit();
    }

    public void implicitCommit(ImplicitCommitHandler handler) {
        transactionManager.implicitCommit(handler);
    }

    public void performSavePoint(String spName, SavePointHandler.Type type) {
        if (savePointHandler == null) {
            savePointHandler = new SavePointHandler(this);
        }
        savePointHandler.perform(spName, type);
    }

    public void clearSavepoint() {
        if (savePointHandler != null) {
            savePointHandler.clearResources();
        }
    }

    public void checkBackupStatus() {
        while (DbleServer.getInstance().isBackupLocked()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        needWaitFinished = true;
    }

    public void rollback() {
        transactionManager.rollback();
    }

    /**
     * lockTable
     *
     * @param rrs
     * @author songdabin
     * @date 2016-7-9
     */
    public void lockTable(RouteResultset rrs) {
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null ||
                nodes[0].getName().equals("")) {
            service.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                    "No shardingNode found ,please check tables defined in schema:" + service.getSchema());
            return;
        }
        LockTablesHandler handler = new LockTablesHandler(this, rrs);
        service.setLocked(true);
        transactionManager.setXaTxEnabled(false, service);
        try {
            handler.execute();
        } catch (Exception e) {
            service.setLocked(false);
            LOGGER.info(String.valueOf(service) + rrs, e);
            service.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
        }
    }

    /**
     * unLockTable
     *
     * @param sql
     * @author songdabin
     * @date 2016-7-9
     */
    public void unLockTable(String sql) {
        UnLockTablesHandler handler = new UnLockTablesHandler(this, this.service.isAutocommit(), sql);
        handler.execute();
    }


    /**
     * {@link ServerConnection#isClosed()} must be true before invoking this
     */
    public void terminate() {
        // XA MUST BE FINISHED
        if ((service.isTxStart() && transactionManager.getXAStage() != null) ||
                needWaitFinished) {
            return;
        }
        for (BackendConnection node : target.values()) {
            node.close("client closed or timeout killed");
        }
        target.clear();
    }

    public void closeAndClearResources(String reason) {
        // XA MUST BE FINISHED
        if (service.isTxStart() && transactionManager.getXAStage() != null) {
            return;
        }
        for (BackendConnection node : target.values()) {
            node.businessClose(reason);
        }
        target.clear();
    }

    public void forceClose(String reason) {
        for (BackendConnection node : target.values()) {
            node.businessClose(reason);
        }
        target.clear();
    }

    public void releaseConnectionIfSafe(MySQLResponseService service, boolean needClosed) {
        RouteResultsetNode node = (RouteResultsetNode) service.getAttachment();
        if (node != null) {
            if ((this.service.isAutocommit() || service.getConnection().isFromSlaveDB()) && !this.service.isTxStart() && !this.service.isLocked()) {
                releaseConnection((RouteResultsetNode) service.getAttachment(), LOGGER.isDebugEnabled(), needClosed);
            }
        }
    }

    public void releaseConnection(RouteResultsetNode rrn, boolean debug, final boolean needClose) {
        if (rrn != null) {
            BackendConnection c = target.remove(rrn);
            if (c != null && !c.isClosed()) {
                if (service.isFlowControlled()) {
                    releaseConnectionFromFlowCntrolled(c);
                }
                if (((MySQLResponseService) c.getService()).isAutocommit()) {
                    c.release();
                } else if (needClose) {
                    //c.rollback();
                    c.close("the need to be closed");
                } else {
                    c.release();
                }
            }
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> iterator = target.entrySet().iterator();
        while (iterator.hasNext()) {
            BackendConnection theCon = iterator.next().getValue();
            if (theCon == con) {
                iterator.remove();
                con.release();
                break;
            }
        }
    }

    public void waitFinishConnection(RouteResultsetNode rrn) {
        BackendConnection c = target.get(rrn);
        if (c != null) {
            BackEndDataCleaner clear = new BackEndDataCleaner((MySQLResponseService) c.getService());
            clear.waitUntilDataFinish();
        }
    }

    // thread may not safe
    public void releaseConnections(final boolean needClosed) {
        boolean debug = LOGGER.isDebugEnabled();
        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, debug, needClosed);
        }
    }

    /**
     * @return previous bound connection
     */
    public BackendConnection bindConnection(RouteResultsetNode key, BackendConnection conn) {
        return target.put(key, conn);
    }

    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        if (conn == null) {
            return false;
        }

        boolean canReUse = false;
        if (conn.isFromSlaveDB() && (node.canRunINReadDB(service.isAutocommit()) &&
                (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
            canReUse = true;
        }

        if (!conn.isFromSlaveDB()) {
            canReUse = true;
        }

        if (canReUse) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found connections in session to use " + conn + " for " + node);
            }
            ((MySQLResponseService) conn.getService()).setAttachment(node);
            return true;
        } else {
            // slave db connection and can't use anymore ,release it
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("release slave connection,can't be used in trasaction  " + conn + " for " + node);
            }
            releaseConnection(node, LOGGER.isDebugEnabled(), false);
        }
        return false;
    }

    protected void kill() {
        AtomicInteger count = new AtomicInteger(0);
        Map<RouteResultsetNode, BackendConnection> toKilled = new HashMap<>();

        for (Map.Entry<RouteResultsetNode, BackendConnection> entry : target.entrySet()) {
            BackendConnection c = entry.getValue();
            if (c != null) {
                toKilled.put(entry.getKey(), c);
                count.incrementAndGet();
            }
        }

        for (Entry<RouteResultsetNode, BackendConnection> en : toKilled.entrySet()) {
            KillConnectionHandler kill = new KillConnectionHandler(en.getValue(), this);
            ServerConfig conf = DbleServer.getInstance().getConfig();
            ShardingNode dn = conf.getShardingNodes().get(en.getKey().getName());
            try {
                dn.getConnectionFromSameSource(en.getValue().getSchema(), en.getValue(), kill, en.getKey());
            } catch (Exception e) {
                LOGGER.info("get killer connection failed for " + en.getKey(), e);
                kill.connectionError(e, null);
            }
        }
    }

    public void clearResources(final boolean needClosed) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear session resources " + this);
        }
        if (!service.isLocked()) {
            this.releaseConnections(needClosed);
        }
        if (!transactionManager.isRetryXa()) {
            transactionManager.setRetryXa(true);
        }
        needWaitFinished = false;
        service.setTxStarted(false);
        service.getAndIncrementXid();
    }

    public boolean closed() {
        return service.getConnection().isClosed();
    }

    public MySQLShardingService getShardingService() {
        return service;
    }

    public String getSessionXaID() {
        return transactionManager.getSessionXaID();
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    public MySQLResponseService freshConn(BackendConnection errConn, ResponseHandler queryHandler) {
        for (final RouteResultsetNode node : this.getTargetKeys()) {
            final BackendConnection mysqlCon = this.getTarget(node);
            if (errConn.equals(mysqlCon)) {
                ServerConfig conf = DbleServer.getInstance().getConfig();
                ShardingNode dn = conf.getShardingNodes().get(node.getName());
                try {
                    BackendConnection newConn = dn.getConnection(dn.getDatabase(), false, errConn.getBackendService().getAttachment());
                    newConn.getBackendService().setXaStatus(errConn.getBackendService().getXaStatus());
                    newConn.getBackendService().setSession(this);
                    newConn.getBackendService().setResponseHandler(queryHandler);
                    errConn.businessClose("error connection change in xa");
                    this.bindConnection(node, newConn);
                    return newConn.getBackendService();
                } catch (Exception e) {
                    return errConn.getBackendService();
                }
            }
        }
        return errConn.getBackendService();
    }

    public boolean handleSpecial(RouteResultset rrs, boolean isSuccess, String errInfo) {
        if (rrs.getSchema() != null) {
            String sql = rrs.getSrcStatement();
            if (service.isTxStart()) {
                service.setTxStarted(false);
                service.getAndIncrementXid();
            }
            if (!isSuccess) {
                LOGGER.warn("DDL execute failed or Session closed, " +
                        "Schema[" + rrs.getSchema() + "],SQL[" + sql + "]" + (errInfo != null ? "errorInfo:" + errInfo : ""));
            }

            if (rrs.isOnline()) {
                LOGGER.info("online ddl skip updating meta and cluster notify, Schema[" + rrs.getSchema() + "],SQL[" + sql + "]" + (errInfo != null ? "errorInfo:" + errInfo : ""));
                return true;
            }
            DDLTraceManager.getInstance().updateDDLStatus(DDLTraceInfo.DDLStage.META_UPDATE, service);
            return ProxyMeta.getInstance().getTmManager().updateMetaData(rrs.getSchema(), rrs.getTable(), sql, isSuccess, rrs.getDdlType());
        } else {
            LOGGER.info("Hint ddl do not update the meta");
            return true;
        }
    }

    /**
     * backend packet server_status change and next round start
     */
    public boolean multiStatementPacket(MySQLPacket packet, byte packetNum) {
        if (this.isMultiStatement.get()) {
            if (packet instanceof OkPacket) {
                ((OkPacket) packet).markMoreResultsExists();
            } else if (packet instanceof EOFPacket) {
                ((EOFPacket) packet).markMoreResultsExists();
            }
            this.getPacketId().set(packetNum);
            return true;
        }
        return false;
    }

    /**
     * backend row eof packet server_status change and next round start
     */
    public boolean multiStatementPacket(byte[] eof, byte packetNum) {
        if (this.getIsMultiStatement().get()) {
            //if there is another statement is need to be executed ,start another round
            eof[7] = (byte) (eof[7] | StatusFlags.SERVER_MORE_RESULTS_EXISTS);

            this.getPacketId().set(packetNum);
            return true;
        }
        return false;
    }


    public boolean multiStatementPacket(byte[] eof) {
        if (this.getIsMultiStatement().get()) {
            //if there is another statement is need to be executed ,start another round
            eof[7] = (byte) (eof[7] | StatusFlags.SERVER_MORE_RESULTS_EXISTS);
            return true;
        }
        return false;
    }

    public boolean multiStatementPacket(MySQLPacket packet) {
        if (this.isMultiStatement.get()) {
            if (packet instanceof OkPacket) {
                ((OkPacket) packet).markMoreResultsExists();
            } else if (packet instanceof EOFPacket) {
                ((EOFPacket) packet).markMoreResultsExists();
            }
            return true;
        }
        return false;
    }


    public void multiStatementNextSql(boolean flag) {
        //todo 多语句回环需要处理
        /*if (flag) {
            this.setRequestTime();
            this.setQueryStartTime(System.currentTimeMillis());
            DbleServer.getInstance().getFrontHandlerQueue().offer((FrontendCommandHandler) service.getHandler());
        }*/
    }


    public byte[] getOkByteArray() {
        OkPacket ok = new OkPacket();
        byte packet = (byte) this.getPacketId().incrementAndGet();
        ok.read(OkPacket.OK);
        ok.setPacketId(packet);
        this.multiStatementPacket(ok, packet);
        return ok.toBytes();
    }

    public void queryCount() {
        queriesCounter.incrementAndGet();
    }

    public void transactionsCount() {
        transactionsCounter.incrementAndGet();
    }

    public void singleTransactionsCount() {
        if (!service.isTxStart()) {
            transactionsCounter.incrementAndGet();
        }
    }

    public void rowCountRolling() {
        rowCountLastSQL = rowCountCurrentSQL;
        rowCountCurrentSQL = -1;
    }

    public void setRowCount(long rowCount) {
        this.rowCountCurrentSQL = rowCount;
    }

    /**
     * reset the session multiStatementStatus
     */
    public void resetMultiStatementStatus() {
        //clear the record
        this.isMultiStatement.set(false);
        this.remingSql = null;
    }

    boolean generalNextStatement(String sql) {
        int index = ParseUtil.findNextBreak(sql);
        if (index + 1 < sql.length() && !ParseUtil.isEOF(sql, index)) {
            this.remingSql = sql.substring(index + 1);
            this.isMultiStatement.set(true);
            return true;
        } else {
            this.remingSql = null;
            this.isMultiStatement.set(false);
            return false;
        }
    }


    public MemSizeController getJoinBufferMC() {
        return joinBufferMC;
    }

    public MemSizeController getOrderBufferMC() {
        return orderBufferMC;
    }

    public MemSizeController getOtherBufferMC() {
        return otherBufferMC;
    }


    public AtomicBoolean getIsMultiStatement() {
        return isMultiStatement;
    }

    public String getRemingSql() {
        return remingSql;
    }

    public AtomicInteger getPacketId() {
        return service.getPacketId();
    }


    public long getQueryStartTime() {
        return queryStartTime;
    }

    public void setQueryStartTime(long queryStartTime) {
        this.queryStartTime = queryStartTime;
    }


    public boolean isTrace() {
        return traceEnable;
    }

    public void setTrace(boolean enable) {
        traceEnable = enable;
    }

    public void setTraceSimpleHandler(ResponseHandler simpleHandler) {
        if (traceEnable || SlowQueryLog.getInstance().isEnableSlowLog()) {
            traceResult.setSimpleHandler(simpleHandler);
        }
    }

    public RouteResultset getComplexRrs() {
        return complexRrs;
    }

    public void setRetryXa(boolean retryXa) {
        transactionManager.setRetryXa(retryXa);
    }

    public boolean isRetryXa() {
        return transactionManager.isRetryXa();
    }

    public long getQueriesCounter() {
        return queriesCounter.get();
    }

    public long getTransactionsCounter() {
        return transactionsCounter.get();
    }

    public void resetCounter() {
        queriesCounter.set(Long.MIN_VALUE);
        transactionsCounter.set(Long.MIN_VALUE);
    }

    public boolean isKilled() {
        return killed;
    }

    public void setKilled(boolean killed) {
        this.killed = killed;
    }

    public boolean isDiscard() {
        return discard;
    }

    public void setDiscard(boolean discard) {
        this.discard = discard;
    }

    public void stopFlowControl() {
        LOGGER.info("Session stop flow control " + this.getFrontConnection());
       /* synchronized (flowControlledBackendConnections) {
            service.setFlowControlled(false);
            for (BackendConnection entry : flowControlledBackendConnections) {
                entry.enableRead();
            }
            flowControlledBackendConnections.clear();
        }*/
    }

    public void startFlowControl(BackendConnection backendConnection) {
        /*synchronized (flowControlledBackendConnections) {
            if (!service.isFlowControlled()) {
                LOGGER.info("Session start flow control " + this.getFrontConnection());
            }
            service.setFlowControlled(true);
            backendConnection.disableRead();
            flowControlledBackendConnections.add(backendConnection);
        }*/
    }

    public void releaseConnectionFromFlowCntrolled(BackendConnection con) {
        /*synchronized (flowControlledBackendConnections) {
            if (flowControlledBackendConnections.remove(con)) {
                con.enableRead();
                if (flowControlledBackendConnections.size() == 0) {
                    service.setFlowControlled(false);
                }
            }
        }*/
    }

}
