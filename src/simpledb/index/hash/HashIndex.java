package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.record.Layout;
import simpledb.record.RID;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    private final Transaction transaction;
    private final String indexName;
    private final Layout layout;
    private Constant searchKey = null;
    private TableScan ts = null;

    public HashIndex(Transaction transaction, String indexName, Layout layout){
        this.transaction = transaction;
        this.indexName = indexName;
        this.layout = layout;
    }

    public static int searchCost(int numBlocks, int rpb) {
        return numBlocks / NUM_BUCKETS;
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        String tableName = indexName + bucket;
        ts = new TableScan(transaction, tableName, layout);
    }

    @Override
    public boolean next() {
        while(ts.next()){
            if(ts.getValue("dataval").equals(searchKey)){
                return true;
            }
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        int blockNum = ts.getInt("block");
        int id = ts.getInt("id");
        return new RID(blockNum, id);
    }

    @Override
    public void insert(Constant dataVal, RID dataRid) {
        beforeFirst(dataVal);
        ts.insert();
        ts.setInt("block", dataRid.getBlockNum());
        ts.setInt("id", dataRid.getSlot());
        ts.setValue("dataval", dataVal);
    }

    @Override
    public void delete(Constant dataVal, RID dataRid) {
        beforeFirst(dataVal);
        while(next()){
            if(getDataRid().equals(dataRid)){
                ts.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if(ts != null){
            ts.close();
        }
    }
}
