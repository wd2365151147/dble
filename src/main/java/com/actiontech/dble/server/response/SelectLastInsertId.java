/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectLastInsertId implements InnerFuncResponse {
    private static final String ORG_NAME = "LAST_INSERT_ID()";
    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);

    public static void response(MySQLShardingService service, String stmt, int aliasIndex) {
        String alias = ParseUtil.parseAlias(stmt, aliasIndex);
        if (alias == null) {
            alias = ORG_NAME;
        }

        ByteBuffer buffer = service.allocate();

        byte packetId = setCurrentPacket(service);
        HEADER.setPacketId(++packetId);
        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields

        FieldPacket field = PacketUtil.getField(alias, ORG_NAME, Fields.FIELD_TYPE_LONGLONG);
        field.setPacketId(++packetId);
        buffer = field.write(buffer, service, true);

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.setPacketId(++packetId);
        buffer = eof.write(buffer, service, true);

        // write rows
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getLastInsertId()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        service.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, service, true);
        boolean multiStatementFlag = service.getSession2().getIsMultiStatement().get();
        // post write
        service.write(buffer);
        service.getSession2().multiStatementNextSql(multiStatementFlag);
    }

    public static byte setCurrentPacket(MySQLShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();

        return packetId;
    }

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField(ORG_NAME, Fields.FIELD_TYPE_LONGLONG));
        return result;
    }

    public List<RowDataPacket> getRows(MySQLShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getLastInsertId()));
        result.add(row);
        return result;
    }
}
