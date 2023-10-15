package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.transaction.Transaction;

import java.util.Map;

public class MetadataManager {
    private static TableManager tm;
    private static ViewManager vm;
    private static StatManager sm;
    private static IndexManager im;
    public MetadataManager(boolean isNew, Transaction transaction) {
        tm = new TableManager(isNew, transaction);
        sm = new StatManager(tm, transaction);
        im = new IndexManager(isNew, tm, sm, transaction);
        vm = new ViewManager(isNew, tm, transaction);
    }

    public void createTable(String tableName, Schema schema, Transaction transaction) {
        tm.createTable(tableName, schema, transaction);
    }

    public Layout getLayout(String tableName, Transaction transaction) {
        return tm.getLayout(tableName, transaction);
    }

    public StatInfo getStatInfo(String tableName, Layout layout, Transaction transaction) {
        return sm.getStatInfo(tableName, layout, transaction);
    }

    public void createView(String tableName, String viewDef, Transaction transaction) {
        vm.createView(tableName, viewDef, transaction);
    }

    public void createIndex(String indexName, String tableName, String fieldName, Transaction transaction) {
        im.createIndex(indexName, tableName, fieldName, transaction);
    }

    public String getViewDefinition(String viewName, Transaction transaction) {
        return vm.getViewDefinition(viewName, transaction);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction transaction) {
        return im.getIndexInfo(tableName, transaction);
    }
}
