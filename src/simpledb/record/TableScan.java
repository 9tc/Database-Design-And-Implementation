package simpledb.record;

import simpledb.file.BlockId;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.transaction.Transaction;

public class TableScan implements UpdateScan {
    private Transaction transaction;
    private Layout layout;
    private RecordPage recordPage;
    private String filename;
    private int currentSlot;
    public TableScan(Transaction transaction, String tableName, Layout layout) {
        this.transaction = transaction;
        this.layout = layout;
        filename = tableName + ".tbl";
        if(transaction.size(filename) == 0){
            moveToNewBlock();
        }else{
            moveToBlock(0);
        }
    }



    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
    public boolean next() {
        currentSlot = recordPage.nextAfter(currentSlot);
        while(currentSlot < 0){
            if(atLastBlock()) {
                return false;
            }
            moveToBlock(recordPage.block().number() + 1);
            currentSlot = recordPage.nextAfter(currentSlot);
        }
        return true;
    }

    @Override
    public int getInt(String fieldName) {
        return recordPage.getInt(currentSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return recordPage.getString(currentSlot, fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        if(layout.schema().type(fieldName) == java.sql.Types.INTEGER){
            return new Constant(getInt(fieldName));
        }else{
            return new Constant(getString(fieldName));
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.schema().hasField(fieldName);
    }

    @Override
    public void close() {
        if(recordPage != null){
            transaction.unpin(recordPage.block());
        }
    }

    @Override
    public void setInt(String fieldName, int value) {
        recordPage.setInt(currentSlot, fieldName, value);
    }

    @Override
    public void setString(String fieldName, String value) {
        recordPage.setString(currentSlot, fieldName, value);
    }

    @Override
    public void setValue(String fieldName, Constant value) {
        if(layout.schema().type(fieldName) == java.sql.Types.INTEGER){
            setInt(fieldName, value.asInteger());
        }else{
            setString(fieldName, value.asString());
        }
    }

    @Override
    public void delete() {
        recordPage.delete(currentSlot);
    }

    @Override
    public void insert() {
        currentSlot = recordPage.insertAfter(currentSlot);
        while(currentSlot < 0){
            if(atLastBlock()){
                moveToNewBlock();
            }else{
                moveToBlock(recordPage.block().number() + 1);
            }
            currentSlot = recordPage.insertAfter(currentSlot);
        }
    }

    @Override
    public RID getRid() {
        return new RID(recordPage.block().number(), currentSlot);
    }

    @Override
    public void moveToRid(RID rid) {
        close();
        BlockId block = new BlockId(filename, rid.blockNumber());
        recordPage = new RecordPage(transaction, block, layout);
        currentSlot = rid.slot();
    }

    private void moveToBlock(int blockNumber) {
        close();
        BlockId block = new BlockId(filename, blockNumber);
        recordPage = new RecordPage(transaction, block, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockId block = transaction.append(filename);
        recordPage = new RecordPage(transaction, block, layout);
        recordPage.format();
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return recordPage.block().number() == transaction.size(filename) - 1;
    }
}
