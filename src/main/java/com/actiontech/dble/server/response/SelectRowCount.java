package com.actiontech.dble.server.response;

import com.actiontech.dble.backend.mysql.PacketUtil;
import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.mysql.FieldPacket;
import com.actiontech.dble.net.mysql.ResultSetHeaderPacket;
import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;
import com.actiontech.dble.util.LongUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2020/3/18.
 */
public class SelectRowCount implements InnerFuncResponse {

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();


    public static void response(MySQLShardingService service) {
        byte packetId = setCurrentPacket(service);
        HEADER.setPacketId(++packetId);
        FIELDS[0] = PacketUtil.getField("ROW_COUNT()", Fields.FIELD_TYPE_LONG);
        FIELDS[0].setPacketId(++packetId);
        EOF.setPacketId(++packetId);

        ByteBuffer buffer = service.allocate();
        buffer = HEADER.write(buffer, service, true);
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, service, true);
        }
        buffer = EOF.write(buffer, service, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getSession2().getRowCount()));
        row.setPacketId(++packetId);
        buffer = row.write(buffer, service, true);
        EOFPacket lastEof = new EOFPacket();
        lastEof.setPacketId(++packetId);
        service.getSession2().multiStatementPacket(lastEof, packetId);
        buffer = lastEof.write(buffer, service, true);
        boolean multiStatementFlag = service.getSession2().getIsMultiStatement().get();
        service.writeDirectly(buffer);
        service.getSession2().multiStatementNextSql(multiStatementFlag);
    }


    public static byte setCurrentPacket(MySQLShardingService service) {
        byte packetId = (byte) service.getSession2().getPacketId().get();
        return packetId;
    }

    @Override
    public List<FieldPacket> getField() {
        List<FieldPacket> result = new ArrayList<>();
        result.add(PacketUtil.getField("ROW_COUNT()", Fields.FIELD_TYPE_LONG));
        return result;
    }


    @Override
    public List<RowDataPacket> getRows(MySQLShardingService service) {
        List<RowDataPacket> result = new ArrayList<>();
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(service.getSession2().getRowCount()));
        result.add(row);
        return result;
    }
}
