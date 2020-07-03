/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.*;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectCurrentUser implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();
    private static final ErrorPacket ERROR = PacketUtil.getShutdown();

    public static void response(MySQLShardingService service) {
        if (DbleServer.getInstance().isOnline()) {

            byte packetId = setCurrentPacket(service);
            HEADER.setPacketId(++packetId);
            FIELDS[0] = PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING);
            FIELDS[0].setPacketId(++packetId);
            EOF.setPacketId(++packetId);

            ByteBuffer buffer = service.allocate();
            buffer = HEADER.write(buffer, service, true);
            for (FieldPacket field : FIELDS) {
                buffer = field.write(buffer, service, true);
            }
            buffer = EOF.write(buffer, service, true);

            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(getUser(service));
            row.setPacketId(++packetId);
            buffer = row.write(buffer, service, true);
            EOFPacket lastEof = new EOFPacket();
            lastEof.setPacketId(++packetId);
            service.getSession2().multiStatementPacket(lastEof, packetId);
            buffer = lastEof.write(buffer, service, true);
            boolean multiStatementFlag = service.getSession2().getIsMultiStatement().get();
            service.write(buffer);
            service.getSession2().multiStatementNextSql(multiStatementFlag);
        } else {
            ERROR.write(service.getConnection());
        }
    }

    private static byte[] getUser(MySQLShardingService service) {
        return StringUtil.encode(service.getUser() + "@%", service.getCharset().getResults());
    }

    public static byte setCurrentPacket(MySQLShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("CURRENT_USER()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(MySQLShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(getUser(service));
        result.add(row);
        return result;
    }
}
