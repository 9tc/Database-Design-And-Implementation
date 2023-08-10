package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexManager {
    private Layout layout;
    private TableManager tm;
    private StatManager sm;

    public IndexManager(boolean isNew, TableManager tm, StatManager sm, Transaction transaction){
        if(isNew){
            Schema schema = new Schema();
            schema.addStringField("indexname", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("tablename", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("fieldname", TableManager.MAX_NAME_LENGTH);
            tm.createTable("idxcat", schema, transaction);
        }

        this.tm = tm;
        this.sm = sm;
        layout = tm.getLayout("idxcat", transaction);
    }

    public void createIndex(String indexName, String tableName, String fieldName, Transaction transaction){
        TableScan ts = new TableScan(transaction, "idxcat", layout);
        ts.insert();
        ts.setString("indexname", indexName);
        ts.setString("tablename", tableName);
        ts.setString("fieldname", fieldName);
        ts.close();
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction transaction){
        Map<String, IndexInfo> result = new HashMap<String, IndexInfo>();
        TableScan ts = new TableScan(transaction, "idxcat", layout);
        while(ts.next()){
            if(ts.getString("tablename").equals(tableName)){
                String indexName = ts.getString("indexname");
                String fieldName = ts.getString("fieldname");
                Layout tableLayout = tm.getLayout(tableName, transaction);
                StatInfo tableStatInfo = sm.getStatInfo(tableName, tableLayout, transaction);
                result.put(fieldName, new IndexInfo(indexName, fieldName, tableLayout.schema(), transaction, tableStatInfo));
            }
        }
        ts.close();
        return result;
    }
}
