package com.actiontech.dble.statistic.backend;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.services.manager.information.tables.statistic.AssociateTablesByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.FrontendByBackendByEntryByUser;
import com.actiontech.dble.services.manager.information.tables.statistic.TableByUserByEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public final class StatisticManager {
    private static final StatisticManager INSTANCE = new StatisticManager();
    private StatisticDisruptor disruptor;
    private static Map<String, StatisticDataHandler> statisticDataHandlers = new HashMap<>(8);
    private static StatisticListener statisticListener = StatisticListener.getInstance();
    private boolean isStart = false;

    // variable
    private volatile boolean enable = SystemConfig.getInstance().getEnableStatistic() == 1;
    private volatile int statisticTableSize = SystemConfig.getInstance().getStatisticTableSize();
    private int statisticQueueSize = SystemConfig.getInstance().getStatisticQueueSize();

    private StatisticManager() {
    }

    public static StatisticManager getInstance() {
        return INSTANCE;
    }

    static {
        statisticDataHandlers.put(FrontendByBackendByEntryByUser.TABLE_NAME, new StatisticCalculation());
        statisticDataHandlers.put(TableByUserByEntry.TABLE_NAME, new StatisticCalculation2());
        statisticDataHandlers.put(AssociateTablesByEntryByUser.TABLE_NAME, new StatisticCalculation3());
        // TODO other handler ....
    }


    // start
    public void start() {
        if (isStart) return;
        ArrayList list = new ArrayList<>(statisticDataHandlers.values());
        disruptor = new StatisticDisruptor(statisticQueueSize, (StatisticDataHandler[]) list.toArray(new StatisticDataHandler[list.size()]));
        statisticListener.start();
        isStart = true;
    }

    // stop
    public void stop() {
        if (!isStart) return;
        statisticListener.stop();
        disruptor.stop();
        isStart = false;
    }

    // push
    public void push(final StatisticEntry entry) {
        disruptor.push(entry);
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
        if (enable) {
            start();
        } else {
            stop();
        }
    }

    public int getStatisticTableSize() {
        return statisticTableSize;
    }

    public void setStatisticTableSize(int statisticTableSize) {
        this.statisticTableSize = statisticTableSize;
    }

    public int getStatisticQueueSize() {
        return statisticQueueSize;
    }

    public StatisticDataHandler getHandler(String key) {
        return statisticDataHandlers.get(key);
    }
}
