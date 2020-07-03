package com.actiontech.dble.services.manager;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.net.mysql.PingPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.route.parser.util.Pair;
import com.actiontech.dble.services.MySQLBasedService;

import java.io.UnsupportedEncodingException;

/**
 * Created by szf on 2020/6/28.
 */
public class ManagerService extends MySQLBasedService {

    private final ManagerQueryHandler handler;

    public ManagerService(AbstractConnection connection) {
        super(connection);
        this.handler = new ManagerQueryHandler(this);
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        this.user = new Pair<>(info.getMysqlAuthPacket().getUser(), "");
        this.userConfig = info.getUserConfig();
        connection.initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
        this.clientFlags = info.getMysqlAuthPacket().getClientFlags();
    }


    @Override
    protected void handleInnerData(byte[] data) {
        switch (data[4]) {
            case MySQLPacket.COM_QUERY:
                //commands.doQuery();
                try {
                    handler.query(proto.getSQL(data, this.getConnection().getCharsetName()));
                } catch (UnsupportedEncodingException e) {
                    writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + this.getConnection().getCharsetName().getClient() + "'");
                }
                break;
            case MySQLPacket.COM_PING:
                //commands.doPing();
                PingPacket.response(this);
                break;
            case MySQLPacket.COM_QUIT:
                //commands.doQuit();
                connection.close("quit cmd");
                break;
            default:
                //commands.doOther();
                this.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public ManagerUserConfig getUserConfig() {
        return (ManagerUserConfig) userConfig;
    }
}
