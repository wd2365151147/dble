/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.parser.ScriptPrepareParse;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;

import java.util.LinkedList;
import java.util.List;

public final class SptPrepare {
    private SptPrepare() {
    }

    private static boolean checksync(String stmt, MySQLShardingService service) {
        return true;
    }

    public static void response(MySQLShardingService service) {
        String stmt = service.getSptPrepare().getExePrepare();
        if (stmt == null) {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
            return;
        }

        if (checksync(stmt, service)) {
            String name = service.getSptPrepare().getName();
            List<String> args = new LinkedList();
            ScriptPrepareParse.parseStmt(stmt, args);
            service.getSptPrepare().setPrepare(name, args);
            service.write(service.writeToBuffer(OkPacket.OK, service.allocate()));
        } else {
            service.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "SQL syntax error");
        }
    }
}
