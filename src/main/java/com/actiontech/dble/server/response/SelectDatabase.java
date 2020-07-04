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
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;
import com.actiontech.dble.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public final class SelectDatabase implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(MySQLShardingService shardingService) {
        byte packetId = setCurrentPacket(shardingService);

        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("DATABASE()", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);
        ByteBuffer buffer = shardingService.allocate();
        buffer = HEADER.write(buffer, shardingService, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, shardingService, true);
        }
        buffer = EOF.write(buffer, shardingService, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(shardingService.getSchema(), shardingService.getCharset().getResults()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, shardingService, true);
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        shardingService.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, shardingService, true);
        boolean multiStatementFlag = shardingService.getSession2().getIsMultiStatement().get();
        shardingService.write(buffer);
        shardingService.getSession2().multiStatementNextSql(multiStatementFlag);
    }

    public static byte setCurrentPacket(MySQLShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }

    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("DATABASE()", Fields.FIELD_TYPE_VAR_STRING));
        return result;
    }

    public List<RowDataPacket> getRows(MySQLShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(service.getSchema(), service.getCharset().getResults()));
        result.add(row);
        return result;
    }
}
