package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class FieldsListHandler implements ResponseHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldsListHandler.class);
    private NonBlockingSession session;
    private RouteResultset rrs;
    private volatile byte packetId;

    public FieldsListHandler(NonBlockingSession session, RouteResultset rrs) {
        this.session = session;
        this.rrs = rrs;
        this.packetId = (byte) session.getPacketId().get();
    }

    public void execute() throws Exception {
        RouteResultsetNode node = rrs.getNodes()[0];
        BackendConnection conn = session.getTarget(node);
        if (session.tryExistsCon(conn, node)) {
            innerExecute(conn, node);
        } else {
            // create new connection
            ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(node.getName());
            dn.getConnection(dn.getDatabase(), session.getSource().isTxStart(), session.getSource().isAutocommit(), node, this, node);
        }
    }

    @Override
    public void connectionError(Throwable e, Object attachment) {
        RouteResultsetNode rrn = (RouteResultsetNode) attachment;
        ErrorPacket errPacket = new ErrorPacket();
        errPacket.setPacketId(++packetId);
        errPacket.setErrNo(ErrorCode.ER_DB_INSTANCE_ABORTING_CONNECTION);
        String errMsg = "can't connect to shardingNode[" + rrn.getName() + "], due to " + e.getMessage();
        errPacket.setMessage(StringUtil.encode(errMsg, session.getSource().getCharset().getResults()));
        LOGGER.warn(errMsg);
        backConnectionErr(errPacket, null, false);
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
        session.bindConnection(node, conn);
        innerExecute(conn, node);
    }

    @Override
    public void errorResponse(byte[] data, BackendConnection conn) {
        ErrorPacket errPkg = new ErrorPacket();
        errPkg.read(data);
        errPkg.setPacketId(++packetId);
        backConnectionErr(errPkg, conn, conn.syncAndExecute());
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        //not happen
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, List<FieldPacket> fieldPacketsNull, byte[] eof, boolean isLeft, BackendConnection conn) {
        ServerConnection source = session.getSource();
        ByteBuffer buffer = session.getSource().allocate();

        List<FieldPacket> fieldPackets = new ArrayList<>();
        for (int i = 0, len = fields.size(); i < len; ++i) {
            byte[] field = fields.get(i);
            field[3] = ++packetId;


            // save field
            FieldPacket fieldPk = new FieldPacket();
            fieldPk.read(field);
            if (rrs.getSchema() != null) {
                fieldPk.setDb(rrs.getSchema().getBytes());
            }
            if (rrs.getTableAlias() != null) {
                fieldPk.setTable(rrs.getTableAlias().getBytes());
            }
            if (rrs.getTable() != null) {
                fieldPk.setOrgTable(rrs.getTable().getBytes());
            }
            fieldPackets.add(fieldPk);
            buffer = fieldPk.write(buffer, source, false);
        }
        eof[3] = ++packetId;
        buffer = source.writeToBuffer(eof, buffer);
        source.write(buffer);
    }

    @Override
    public boolean rowResponse(byte[] rowNull, RowDataPacket rowPacket, boolean isLeft, BackendConnection conn) {
        //not happen
        return false;
    }

    @Override
    public void rowEofResponse(byte[] eof, boolean isLeft, BackendConnection conn) {
        //not happen
    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        //not happen
    }

    private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn, boolean syncFinished) {
        ServerConnection source = session.getSource();
        UserName errUser = source.getUser();
        String errHost = source.getHost();
        int errPort = source.getLocalPort();
        String errMsg = " errNo:" + errPkg.getErrNo() + " " + new String(errPkg.getMessage());
        if (conn != null) {
            LOGGER.info("execute sql err :" + errMsg + " con:" + conn +
                    " frontend host:" + errHost + "/" + errPort + "/" + errUser);
            if (syncFinished) {
                session.releaseConnectionIfSafe(conn, false);
            } else {
                conn.closeWithoutRsp("unfinished sync");
                if (conn.getAttachment() == null) {
                    session.getTargetMap().remove(conn.getAttachment());
                }
            }
        }
        source.setTxInterrupt(errMsg);
        errPkg.write(source);
    }

    private void innerExecute(BackendConnection conn, RouteResultsetNode node) {
        conn.setResponseHandler(this);
        conn.setSession(session);
        conn.execute(node, session.getSource(), session.getSource().isAutocommit());
    }


}
