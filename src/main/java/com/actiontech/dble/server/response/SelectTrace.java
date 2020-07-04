/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;

import java.nio.ByteBuffer;

public final class SelectTrace {
    private SelectTrace() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void response(MySQLShardingService service) {
        byte packetId = setCurrentPacket(service);
        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("@@trace", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);


        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(service.getSession2().isTrace() ? "1".getBytes() : "0".getBytes());
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        service.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, service, true);
        boolean multiStatementFlag = service.getSession2().getIsMultiStatement().get();
        service.write(buffer);
        service.getSession2().multiStatementNextSql(multiStatementFlag);
    }


    public static byte setCurrentPacket(MySQLShardingService service) {
        return (byte) service.getSession2().getPacketId().get();
    }

}
