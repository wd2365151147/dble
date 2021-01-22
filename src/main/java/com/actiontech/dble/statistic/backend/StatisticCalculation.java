package com.actiontech.dble.statistic.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class StatisticCalculation implements StatisticDataHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticCalculation.class);

    Map<String, Record> records = new LinkedHashMap<>(1024);
    int entryId = 0;

    public StatisticCalculation() {
    }

    @Override
    public void onEvent(Event event, long l, boolean b) throws Exception {
        // LOGGER.info("consuming:{}", event.getEntry().toString());
        check();
        handle(event.getEntry());
    }

    public void check() {
        synchronized (records) {
            int removeIndex;
            if ((removeIndex = records.values().size() - StatisticManager.getInstance().getstatisticTableSize()) > 0) {
                Iterator<String> iterator = records.keySet().iterator();
                while (removeIndex-- > 0) {
                    iterator.next();
                    iterator.remove();
                }
            }
        }
    }

    public void handle(StatisticEntry entry) {
        synchronized (records) {
            if (entry instanceof StatisticTxEntry) {
                ((StatisticTxEntry) entry).getEntryList().
                        stream().
                        flatMap(k -> k.getBackendSqlEntrys().values().stream()).
                        collect(Collectors.toList()).
                        stream().
                        forEach(v -> {
                            String key = v.getKey();
                            Record currRecord;
                            boolean isNew = false;
                            if (isNew = ((currRecord = records.get(key)) == null)) {
                                currRecord = new Record(++entryId, entry.getFrontend(), v.getBackend());
                            }
                            currRecord.addTx(v.getRows(), v.getDuration());
                            if (isNew) {
                                records.put(key, currRecord);
                            }
                        });
            } else if (entry instanceof StatisticBackendSqlEntry) {
                StatisticBackendSqlEntry backendSqlEntry = (StatisticBackendSqlEntry) entry;
                String key = backendSqlEntry.getKey();
                Record currRecord;
                boolean isNew = false;
                if (isNew = ((currRecord = records.get(key)) == null)) {
                    currRecord = new Record(++entryId, entry.getFrontend(), backendSqlEntry.getBackend());
                }
                switch (backendSqlEntry.getSqlType()) {
                    case 4:
                        currRecord.addInsert(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                        break;
                    case 11:
                        currRecord.addUpdate(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                        break;
                    case 3:
                        currRecord.addDelete(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                        break;
                    case 7:
                        currRecord.addSelect(backendSqlEntry.getRows(), backendSqlEntry.getDuration());
                        break;
                    default:
                        // ignore
                        break;
                }
                if (isNew) {
                    records.put(key, currRecord);
                }
            }
        }
    }

    @Override
    public void clear() {
        synchronized (records) {
            records.clear();
            entryId = 0;
        }
    }

    @Override
    public Map<String, Record> getList() {
        return new HashMap<>(records);
    }

    // sql_statistic_by_frontend_by_backend_by_entry_by_user
    public class Record {
        int entry;
        FrontendInfo frontend;
        StatisticEntry.BackendInfo backend;

        int txCount = 0;
        long txRows = 0;
        long txTime = 0L;

        int insertCount = 0;
        long insertRows = 0;
        long insertTime = 0L;

        int updateCount = 0;
        long updateRows = 0;
        long updateTime = 0L;

        int deleteCount = 0;
        long deleteRows = 0;
        long deleteTime = 0L;

        int selectCount = 0;
        long selectRows = 0;
        long selectTime = 0L;

        long lastUpdateTime = 0L;

        public Record(int entry, FrontendInfo frontend, StatisticEntry.BackendInfo backend) {
            this.entry = entry;
            this.frontend = frontend;
            this.backend = backend;
        }

        public void addTx(long row, long time) {
            txCount += 1;
            txRows += row;
            txTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addInsert(long row, long time) {
            insertCount += 1;
            insertRows += row;
            insertTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addUpdate(long row, long time) {
            updateCount += 1;
            updateRows += row;
            updateTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addDelete(long row, long time) {
            deleteCount += 1;
            deleteRows += row;
            deleteTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void addSelect(long row, long time) {
            selectCount += 1;
            selectRows += row;
            selectTime += time;
            lastUpdateTime = System.currentTimeMillis();
        }

        public int getEntry() {
            return entry;
        }

        public FrontendInfo getFrontend() {
            return frontend;
        }

        public StatisticEntry.BackendInfo getBackend() {
            return backend;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public void setEntry(int entry) {
            this.entry = entry;
        }

        public void setFrontend(FrontendInfo frontend) {
            this.frontend = frontend;
        }

        public void setBackend(StatisticEntry.BackendInfo backend) {
            this.backend = backend;
        }

        public int getInsertCount() {
            return insertCount;
        }

        public long getInsertRows() {
            return insertRows;
        }

        public long getInsertTime() {
            return insertTime;
        }

        public int getUpdateCount() {
            return updateCount;
        }

        public long getUpdateRows() {
            return updateRows;
        }


        public int getDeleteCount() {
            return deleteCount;
        }

        public long getDeleteRows() {
            return deleteRows;
        }

        public long getDeleteTime() {
            return deleteTime;
        }

        public int getSelectCount() {
            return selectCount;
        }

        public long getSelectRows() {
            return selectRows;
        }

        public long getSelectTime() {
            return selectTime;
        }

        public void setSelectTime(long selectTime) {
            this.selectTime = selectTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public int getTxCount() {
            return txCount;
        }

        public long getTxRows() {
            return txRows;
        }

        public long getTxTime() {
            return txTime;
        }
    }
}
