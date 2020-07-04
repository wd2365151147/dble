/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.config.Versions;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;

import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class SelectVersionComment {
    private SelectVersionComment() {
    }

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    public static void response(AbstractService service) {

        byte packetId = setCurrentPacket(service);
        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("@@VERSION_COMMENT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);

        ByteBuffer buffer = service.allocate();

        // write header
        buffer = HEADER.write(buffer, service, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }

        // write eof
        buffer = EOF.write(buffer, service, true);

        // write rows

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(Versions.VERSION_COMMENT);
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        boolean multiStatementFlag = false;
        if (service instanceof MySQLShardingService) {
            multiStatementFlag = ((MySQLShardingService) service).getSession2().getIsMultiStatement().get();
            ((MySQLShardingService) service).getSession2().multiStatementPacket(lastEof, packetId);
        }
        buffer = lastEof.write(buffer, service, true);

        // post write
        service.write(buffer);
        if (service instanceof MySQLShardingService) {
            ((MySQLShardingService) service).getSession2().multiStatementNextSql(multiStatementFlag);
        }

    }


    public static byte setCurrentPacket(AbstractService service) {
        if (service instanceof MySQLShardingService) {
            return (byte) ((MySQLShardingService) service).getSession2().getPacketId().get();
        }
        return 0;
    }

}
