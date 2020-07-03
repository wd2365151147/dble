package com.actiontech.dble.services.mysqlsharding;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.BackendConnection;
import com.actiontech.dble.net.handler.FrontendPrepareHandler;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import com.actiontech.dble.server.parser.ServerParse;
import com.actiontech.dble.server.response.InformationSchemaProfiling;
import com.actiontech.dble.server.util.SchemaUtil;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.statistic.CommandCount;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by szf on 2020/6/18.
 */
public class MySQLShardingService extends MySQLBasedService {

    protected static final Logger LOGGER = LoggerFactory.getLogger(MySQLShardingService.class);

    private Queue<byte[]> blobDataQueue = new ConcurrentLinkedQueue<byte[]>();

    private final ServerQueryHandler handler;

    private final ServerLoadDataInfileHandler loadDataInfileHandler;

    private final FrontendPrepareHandler prepareHandler;

    private final MySQLProtoLogicHandler protoLogicHandler;

    private final MySQLShardingSQLHandler shardingSQLHandler;

    protected final CommandCount commands;

    protected String executeSql;
    protected UserName user;
    private long clientFlags;

    private volatile boolean autocommit;
    private volatile boolean txStarted;
    private volatile boolean txChainBegin;
    private volatile boolean txInterrupted;
    private volatile String txInterruptMsg = "";

    private volatile int txIsolation;

    protected boolean isAuthenticated;

    private AtomicLong txID;

    private volatile boolean isLocked = false;

    private long lastInsertId;

    protected String schema;

    private volatile boolean multiStatementAllow = false;

    protected volatile Map<String, String> usrVariables;
    protected volatile Map<String, String> sysVariables;

    private final NonBlockingSession session;


    public MySQLShardingService(AbstractConnection connection) {
        super(connection);

        this.handler = new ServerQueryHandler(this);
        this.loadDataInfileHandler = new ServerLoadDataInfileHandler(this);
        this.prepareHandler = new ServerPrepareHandler(this);
        this.session = new NonBlockingSession(this);
        this.commands = connection.getProcessor().getCommands();
        this.protoLogicHandler = new MySQLProtoLogicHandler(this);
        this.shardingSQLHandler = new MySQLShardingSQLHandler(this);
    }

    public void query(String sql) {
        this.handler.query(sql);
    }

    @Override
    protected void handleInnerData(byte[] data) {
        getSession2().startProcess();
       /* if (isAuthSwitch.compareAndSet(true, false)) {
            commands.doOther();
            sc.changeUserAuthSwitch(data, changeUserPacket);
            return;
        }*/
        switch (data[4]) {
            case MySQLPacket.COM_INIT_DB:
                commands.doInitDB();
                protoLogicHandler.initDB(data);
                break;
            case MySQLPacket.COM_QUERY:
                commands.doQuery();
                protoLogicHandler.query(data);
                break;
            case MySQLPacket.COM_PING:
                commands.doPing();
                protoLogicHandler.ping();
                break;
            case MySQLPacket.COM_QUIT:
                commands.doQuit();
                connection.close("quit cmd");
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                commands.doKill();
                protoLogicHandler.kill(data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                commands.doStmtPrepare();
                String prepareSql = protoLogicHandler.stmtPrepare(data);
                // record SQL
                if (prepareSql != null) {
                    this.setExecuteSql(prepareSql);
                    prepareHandler.prepare(prepareSql);
                }
                break;
            case MySQLPacket.COM_STMT_RESET:
                commands.doStmtReset();
                prepareHandler.reset(data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                commands.doStmtExecute();
                this.stmtExecute(data, blobDataQueue);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                commands.doHeartbeat();
                protoLogicHandler.heartbeat(data);
                break;
            case MySQLPacket.COM_SET_OPTION:
                commands.doOther();
                protoLogicHandler.setOption(data);
                break;
            case MySQLPacket.COM_CHANGE_USER:
                //todo 如果是切换用户，那么就转到验证的service里面去
                /*commands.doOther();
                changeUserPacket = new ChangeUserPacket(sc.getClientFlags(), CharsetUtil.getCollationIndex(sc.getCharset().getCollation()));
                sc.changeUser(data, changeUserPacket, isAuthSwitch);*/
                break;
            case MySQLPacket.COM_RESET_CONNECTION:
                commands.doOther();
                protoLogicHandler.resetConnection();
                break;
            default:
                commands.doOther();
                writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }


    public void execute(String sql, int type) {
        if (connection.isClosed()) {
            LOGGER.info("ignore execute ,server connection is closed " + this);
            return;
        }
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, txInterruptMsg);
            return;
        }
        session.setQueryStartTime(System.currentTimeMillis());

        String db = this.schema;

        SchemaConfig schemaConfig = null;
        if (db != null) {
            schemaConfig = DbleServer.getInstance().getConfig().getSchemas().get(db);
            if (schemaConfig == null) {
                writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown Database '" + db + "'");
                return;
            }
        }
        //fix navicat
        // SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage`
        // FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ
        if (ServerParse.SELECT == type && sql.contains(" INFORMATION_SCHEMA.PROFILING ") && sql.contains("CONCAT(ROUND(SUM(DURATION)/")) {
            InformationSchemaProfiling.response(this);
            return;
        }
        shardingSQLHandler.routeEndExecuteSQL(sql, type, schemaConfig);

    }


    public void stmtExecute(byte[] data, Queue<byte[]> dataqueue) {
        byte[] sendData = dataqueue.poll();
        while (sendData != null) {
            this.stmtSendLongData(sendData);
            sendData = dataqueue.poll();
        }
        if (prepareHandler != null) {
            prepareHandler.execute(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void stmtSendLongData(byte[] data) {
        if (prepareHandler != null) {
            prepareHandler.sendLongData(data);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    public void setTxInterrupt(String msg) {
        if ((!autocommit || txStarted) && !txInterrupted) {
            txInterrupted = true;
            this.txInterruptMsg = "Transaction error, need to rollback.Reason:[" + msg + "]";
        }
    }


    public void innerCleanUp() {
        //rollback and unlock tables  means close backend conns;
        Iterator<BackendConnection> connIterator = session.getTargetMap().values().iterator();
        while (connIterator.hasNext()) {
            BackendConnection conn = connIterator.next();
            conn.businessClose("com_reset_connection");
            connIterator.remove();
        }

        isLocked = false;
        txChainBegin = false;
        txStarted = false;
        txInterrupted = false;

        this.sysVariables.clear();
        this.usrVariables.clear();
        autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        txIsolation = SystemConfig.getInstance().getTxIsolation();
        setCharacterSet(SystemConfig.getInstance().getCharset());

        lastInsertId = 0;
        //prepare
        if (prepareHandler != null) {
            prepareHandler.clear();
        }
    }

    public void initCharsetIndex(int ci) {
        String name = CharsetUtil.getCharset(ci);
        if (name != null) {
            connection.setCharacterSet(name);
        }
    }

    public void routeSystemInfoAndExecuteSQL(String stmt, SchemaUtil.SchemaInfo schemaInfo, int sqlType) {
        this.shardingSQLHandler.routeSystemInfoAndExecuteSQL(stmt, schemaInfo, sqlType);
    }

    public void initFromAuthInfo(AuthResultInfo info) {

        AuthPacket auth = info.getMysqlAuthPacket();

        this.userConfig = info.getUserConfig();
        this.user = new UserName(auth.getUser(), auth.getTenant());

        SystemConfig sys = SystemConfig.getInstance();
        txIsolation = sys.getTxIsolation();

        this.initCharsetIndex(auth.getCharsetIndex());
        multiStatementAllow = auth.isMultStatementAllow();
        clientFlags = auth.getClientFlags();

        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            proto = new MySQLProtoHandlerImpl(true);
        } else {
            proto = new MySQLProtoHandlerImpl(false);
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(this).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }
    }

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage((byte) 1, vendorCode, msg);
    }

    public void writeErrMessage(byte id, int vendorCode, String msg) {
        writeErrMessage(id, vendorCode, "HY000", msg);
    }

    protected void writeErrMessage(byte id, int vendorCode, String sqlState, String msg) {
        markFinished();
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(id);
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, connection.getCharsetName().getResults()));
        err.setMessage(StringUtil.encode(msg, connection.getCharsetName().getResults()));
        err.write(connection);
    }

    public void writeErrMessage(String sqlState, String msg, int vendorCode) {
        byte packetId = (byte) this.getSession2().getPacketId().get();
        writeErrMessage(++packetId, vendorCode, sqlState, msg);
        if (session.isDiscard() || session.isKilled()) {
            session.setKilled(false);
            session.setDiscard(false);
        }
    }


    public void markFinished() {
        if (session != null) {
            session.setStageFinished();
        }
    }

    public boolean isTxStarted() {
        return txStarted;
    }

    public void setTxStarted(boolean txStarted) {
        this.txStarted = txStarted;
    }

    public boolean isTxChainBegin() {
        return txChainBegin;
    }

    public void setTxChainBegin(boolean txChainBegin) {
        this.txChainBegin = txChainBegin;
    }

    public boolean isTxInterrupted() {
        return txInterrupted;
    }

    public void setTxInterrupted(boolean txInterrupted) {
        this.txInterrupted = txInterrupted;
    }

    public String getTxInterruptMsg() {
        return txInterruptMsg;
    }

    public void setTxInterruptMsg(String txInterruptMsg) {
        this.txInterruptMsg = txInterruptMsg;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public boolean isTxStart() {
        return txStarted;
    }

    public UserName getUser() {
        return user;
    }

    public String getExecuteSql() {
        return executeSql;
    }

    public void setExecuteSql(String executeSql) {
        this.executeSql = executeSql;
    }

    public boolean isMultiStatementAllow() {
        return multiStatementAllow;
    }

    public void setMultiStatementAllow(boolean multiStatementAllow) {
        this.multiStatementAllow = multiStatementAllow;
    }

    public NonBlockingSession getSession2() {
        return session;
    }

    public ShardingUserConfig getUserConfig() {
        return (ShardingUserConfig) userConfig;
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setCharacterSet(String name) {
        connection.setCharacterSet(name);
    }


    public boolean isLocked() {
        return isLocked;
    }

    public void setLocked(boolean locked) {
        isLocked = locked;
    }

    public long getAndIncrementXid() {
        return txID.getAndIncrement();
    }


    public Map<String, String> getUsrVariables() {
        return usrVariables;
    }

    public void setUsrVariables(Map<String, String> usrVariables) {
        this.usrVariables = usrVariables;
    }
}
