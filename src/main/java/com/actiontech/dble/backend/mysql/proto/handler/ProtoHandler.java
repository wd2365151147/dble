package com.actiontech.dble.backend.mysql.proto.handler;

import com.actiontech.dble.net.mysql.CharsetNames;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Created by szf on 2020/6/16.
 */
public interface ProtoHandler {

    ProtoHandlerResult handle(ByteBuffer dataBuffer, int dataBufferOffset);

    String getSQL(byte[] data, CharsetNames charsetNames) throws UnsupportedEncodingException;


}
