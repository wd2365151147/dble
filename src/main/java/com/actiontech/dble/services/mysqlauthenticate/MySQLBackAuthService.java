package com.actiontech.dble.services.mysqlauthenticate;

import com.actiontech.dble.backend.pool.PooledConnectionListener;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.ConnectionException;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.connection.PooledConnection;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.net.service.AuthService;
import com.actiontech.dble.services.MySQLBasedService;
import com.actiontech.dble.services.factorys.BusinessServiceFactory;
import com.actiontech.dble.services.mysqlauthenticate.plugin.BackendDefaulPlugin;
import com.actiontech.dble.services.mysqlauthenticate.plugin.CachingSHA2Pwd;
import com.actiontech.dble.services.mysqlauthenticate.plugin.MySQLAuthPlugin;
import com.actiontech.dble.services.mysqlauthenticate.plugin.NativePwd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.actiontech.dble.config.ErrorCode.ER_ACCESS_DENIED_ERROR;


/**
 * Created by szf on 2020/6/19.
 */
public class MySQLBackAuthService extends MySQLBasedService implements AuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackAuthService.class);

    private volatile MySQLAuthPlugin plugin;

    private volatile String user;
    private volatile String schema;
    private volatile String passwd;
    private volatile PooledConnectionListener listener;

    public MySQLBackAuthService(AbstractConnection connection, String user, String schema, String passwd, PooledConnectionListener listener) {
        super(connection);
        plugin = new BackendDefaulPlugin(connection);
    }

    public void initFromAuthInfo(AuthResultInfo info) {

    }

    @Override
    protected void handleInnerData(byte[] data) {
        try {
            switch (plugin.handleBackData(data)) {
                case caching_sha2_password:
                    this.plugin = new CachingSHA2Pwd(plugin);
                    plugin.authenticate(user, passwd, schema,++data[3]);
                    break;
                case mysql_native_password:
                    this.plugin = new NativePwd(plugin);
                    plugin.authenticate(user, passwd, schema,++data[3]);
                    break;
                case plugin_same_with_default:
                    checkForResult();
                default:
                    String authPluginErrorMessage = "Client don't support the password plugin ,please check the default auth Plugin";
                    LOGGER.warn(authPluginErrorMessage);
                    throw new RuntimeException(authPluginErrorMessage);
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            if (listener != null) {
                listener.onCreateFail((PooledConnection) connection, e);
            }
        }
    }

    public void checkForResult() {
        if (plugin.getInfo() == null) {
            return;
        }
        if (plugin.getInfo().isSuccess()) {
            connection.setService(BusinessServiceFactory.getBackendBusinessService(plugin.getInfo(), connection));
            boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & plugin.getHandshakePacket().getServerCapabilities());
            boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
            if (clientCompress && usingCompress) {
                connection.setSupportCompress(true);
            }
            if (listener != null) {
                listener.onCreateSuccess((PooledConnection) connection);
            }
        } else {
            throw new ConnectionException(ER_ACCESS_DENIED_ERROR, plugin.getInfo().getErrorMsg());
        }
    }


}
