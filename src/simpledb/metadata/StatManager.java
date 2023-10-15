package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatManager {
    private final TableManager tm;
    private Map<String, StatInfo> tableStats;
    private int numCalls;

    public StatManager(TableManager tm, Transaction transaction){
        this.tm = tm;
        refreshStatistics(transaction);
    }

    public synchronized StatInfo getStatInfo(String tableName, Layout layout, Transaction transaction){
        ++numCalls;
        if(numCalls > 100) refreshStatistics(transaction);

        StatInfo si = tableStats.get(tableName);
        if(si == null){
            si = calcTableStats(tableName, layout, transaction);
            tableStats.put(tableName, si);
        }
        return si;
    }

    private void refreshStatistics(Transaction transaction) {
        tableStats = new HashMap<>();
        numCalls = 0;
        Layout tcatLayout = tm.getLayout("tblcat", transaction);
        TableScan tcatScan = new TableScan(transaction, "tblcat", tcatLayout);
        while(tcatScan.next()){
            String tableName = tcatScan.getString("tblname");
            Layout layout = tm.getLayout(tableName, transaction);
            StatInfo si = calcTableStats(tableName, layout, transaction);
            tableStats.put(tableName, si);
        }
        tcatScan.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction transaction){
        int numRecords = 0;
        int numBlocks = 0;
        TableScan ts = new TableScan(transaction, tableName, layout);
        while(ts.next()){
            numRecords++;
            numBlocks = ts.getRid().getBlockNum() + 1;
        }
        ts.close();
        return new StatInfo(numBlocks, numRecords);
    }
}
