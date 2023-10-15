package simpledb.plan;

import simpledb.metadata.MetadataManager;
import simpledb.metadata.StatInfo;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

public class TablePlan implements Plan{
    private final Transaction transaction;
    private final String tableName;
    private final Layout layout;
    private final StatInfo si;

    public TablePlan(Transaction transaction, String tableName, MetadataManager metadataManager){
        this.transaction = transaction;
        this.tableName = tableName;
        this.layout = metadataManager.getLayout(tableName, transaction);
        this.si = metadataManager.getStatInfo(tableName, layout, transaction);

    }
    @Override
    public Scan open() {
        return new TableScan(transaction, tableName, layout);
    }

    @Override
    public int blocksAccessed() {
        return si.getAccessedBlocks();
    }

    @Override
    public int recordsOutput() {
        return si.getNumRecords();
    }

    @Override
    public int distinctValues(String fieldName) {
        return si.distinctValues(fieldName);
    }

    @Override
    public Schema getSchema() {
        return layout.getSchema();
    }
}
