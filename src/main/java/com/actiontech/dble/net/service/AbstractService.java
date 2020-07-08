package com.actiontech.dble.net.service;


import com.actiontech.dble.backend.mysql.ByteUtil;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandler;
import com.actiontech.dble.backend.mysql.proto.handler.ProtoHandlerResult;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by szf on 2020/6/16.
 */
public abstract class AbstractService implements Service {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractService.class);
    protected final ConcurrentLinkedQueue<ServiceTask> taskQueue = new ConcurrentLinkedQueue<>();
    protected ServiceTask currentTask = null;
    protected volatile ProtoHandler proto;

    private final AtomicBoolean executing = new AtomicBoolean(false);


    protected AbstractConnection connection;

    private AtomicInteger packetId = new AtomicInteger(0);

    public AbstractService(AbstractConnection connection) {
        this.connection = connection;
    }

    @Override
    public void handle(ByteBuffer dataBuffer) {

        boolean hasReming = true;
        int offset = 0;
        int totalsize = 0;
        while (hasReming) {
            ProtoHandlerResult result = proto.handle(dataBuffer, offset);
            switch (result.getCode()) {
                case REACH_END_BUFFER:
                    connection.readReachEnd();
                    byte[] packetData = result.getPacketData();
                    if (packetData != null) {
                        //LOGGER.debug(" get the packet of length " + packetData.length + " of connection " + connection.toString());
                        totalsize += packetData.length;
                        TaskCreate(packetData);
                    }
                    dataBuffer.clear();
                    //LOGGER.debug("get OUT OF THE READ BECAUSE OF THE REACH_END_BUFFER");
                    hasReming = false;
                    break;
                case BUFFER_PACKET_UNCOMPLETE:
                    connection.compactReadBuffer(dataBuffer, result.getOffset());
                    //LOGGER.debug("get OUT OF THE READ BECAUSE OF THE BUFFER_PACKET_UNCOMPLETE");
                    hasReming = false;
                    break;
                case BUFFER_NOT_BIG_ENOUGH:
                    connection.ensureFreeSpaceOfReadBuffer(dataBuffer, result.getOffset(), result.getPacketLength());
                    //LOGGER.debug("get OUT OF THE READ BECAUSE OF THE BUFFER_NOT_BIG_ENOUGH");
                    hasReming = false;
                    break;
                case STLL_DATA_REMING:
                    totalsize += result.getPacketData().length;
                    TaskCreate(result.getPacketData());
                    offset = result.getOffset();
                    continue;
            }
            //LOGGER.info("the read end of the result is +++++++++++++++++++++ " + totalsize);
        }
    }

    private void TaskCreate(byte[] packetData) {
        ServiceTask task = new ServiceTask(packetData, this);
        taskQueue.offer(task);
        TaskToTotalQueue(task);
    }

    protected abstract void TaskToTotalQueue(ServiceTask task);


    @Override
    public void execute(ServiceTask task) {
        task.increasePriority();
        handleData(task);
    }

    public void cleanup() {
        this.currentTask = null;
        this.taskQueue.clear();
    }

    public void register() throws IOException {

    }

    public abstract void handleData(ServiceTask task);

    public int nextPacketId() {
        LOGGER.info("get packetid increment " + packetId.get(),new Exception("test"));
        return packetId.incrementAndGet();
    }

    public void setPacketId(int packetId) {
        this.packetId.set(packetId);
    }

    public AtomicInteger getPacketId() {
        return packetId;
    }

    public AbstractConnection getConnection() {
        return connection;
    }

    public ByteBuffer allocate() {
        return this.connection.allocate();
    }

    public ByteBuffer allocate(int size) {
        return this.connection.allocate(size);
    }

    public void writeDirectly(ByteBuffer buffer) {
        this.connection.write(buffer);
    }

    public void writeDirectly(byte[] data) {
        ByteBuffer buffer = connection.allocate();
        if (data.length >= MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE) {
            ByteBuffer writeBuffer = writeBigPackageToBuffer(data, buffer);
            this.writeDirectly(writeBuffer);
        } else {
            ByteBuffer writeBuffer = writeToBuffer(data, buffer);
            this.writeDirectly(writeBuffer);
        }
    }

    public ByteBuffer write(byte[] data, ByteBuffer buffer) {

        return null;
    }

    public void write(MySQLPacket packet) {
        packet.bufferWrite(connection);
    }

    public void recycleBuffer(ByteBuffer buffer) {
        this.connection.getProcessor().getBufferPool().recycle(buffer);
    }


    public ByteBuffer writeBigPackageToBuffer(byte[] data, ByteBuffer buffer) {
        int srcPos;
        byte[] singlePacket;
        singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
        System.arraycopy(data, 0, singlePacket, 0, MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        srcPos = MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE;
        int length = data.length;
        length -= (MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE);
        ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
        singlePacket[3] = data[3];
        buffer = writeToBuffer(singlePacket, buffer);
        while (length >= MySQLPacket.MAX_PACKET_SIZE) {
            singlePacket = new byte[MySQLPacket.MAX_PACKET_SIZE + MySQLPacket.PACKET_HEADER_SIZE];
            ByteUtil.writeUB3(singlePacket, MySQLPacket.MAX_PACKET_SIZE);
            singlePacket[3] = (byte) nextPacketId();
            System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, MySQLPacket.MAX_PACKET_SIZE);
            srcPos += MySQLPacket.MAX_PACKET_SIZE;
            length -= MySQLPacket.MAX_PACKET_SIZE;
            buffer = writeToBuffer(singlePacket, buffer);
        }
        singlePacket = new byte[length + MySQLPacket.PACKET_HEADER_SIZE];
        ByteUtil.writeUB3(singlePacket, length);
        singlePacket[3] = (byte) nextPacketId();
        System.arraycopy(data, srcPos, singlePacket, MySQLPacket.PACKET_HEADER_SIZE, length);
        buffer = writeToBuffer(singlePacket, buffer);
        return buffer;
    }

    public boolean isFlowControlled() {
        return this.connection.isFlowControlled();
    }

    public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
        return connection.checkWriteBuffer(buffer, capacity, writeSocketIfFull);
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        return connection.writeToBuffer(src, buffer);
    }


}
