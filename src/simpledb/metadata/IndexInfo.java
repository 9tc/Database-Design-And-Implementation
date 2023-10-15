package simpledb.metadata;

import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.transaction.Transaction;

public class IndexInfo {
    private final String indexName;
    private final String fieldName;
    private final Transaction transaction;
    private final Schema tableSchema;
    private final Layout indexLayout;
    private final StatInfo si;

    public IndexInfo(String indexName, String fieldName, Schema schema, Transaction transaction, StatInfo si){
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.tableSchema = schema;
        this.transaction = transaction;
        this.indexLayout = createIndexLayout();
        this.si = si;
    }

    public Index open(){
        return new HashIndex(transaction, indexName, indexLayout);
    }

    public int blocksAccessed(){
        int rpb = transaction.blockSize() / indexLayout.getSlotSize();
        int numBlocks = si.getNumRecords() / rpb;
        return HashIndex.searchCost(numBlocks, rpb);
    }

    public int distinctValues(String fieldName){
        return this.fieldName.equals(fieldName) ? 1 : si.distinctValues(this.fieldName);
    }

    private Layout createIndexLayout(){
        Schema schema = new Schema();
        schema.addIntField("block");
        schema.addIntField("id");
        if(this.tableSchema.type(this.fieldName) == java.sql.Types.INTEGER) {
            schema.addIntField("dataval");
        }else {
            schema.addStringField("dataval", tableSchema.length(this.fieldName));
        }
        return new Layout(schema);
    }

    public int recordsOutput() {
        return si.getNumRecords() / si.distinctValues(fieldName);
    }
}
