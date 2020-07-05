/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.server.handler;

import com.actiontech.dble.log.transaction.TxnLogHelper;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.services.mysqlsharding.MySQLShardingService;

public final class BeginHandler {
    private BeginHandler() {
    }

    public static void handle(String stmt, MySQLShardingService service) {
        if (service.isTxStart() || !service.isAutocommit()) {
            service.beginInTx(stmt);
        } else {
            service.setTxStarted(true);
            TxnLogHelper.putTxnLog(service, stmt);
            boolean multiStatementFlag = service.getSession2().getIsMultiStatement().get();
            service.write(service.writeToBuffer(service.getSession2().getOkByteArray(), service.allocate()));
            service.getSession2().multiStatementNextSql(multiStatementFlag);
        }
    }
}
