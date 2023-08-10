package simpledb.record;

import simpledb.file.BlockId;
import simpledb.transaction.Transaction;

public class RecordPage {
    public static final int EMPTY = 0;
    public static final int INUSE = 1;
    public Transaction transaction;
    public BlockId block;
    public Layout layout;
    public RecordPage(Transaction transaction, BlockId block, Layout layout) {
        this.transaction = transaction;
        this.block = block;
        this.layout = layout;
        transaction.pin(block);
    }

    public int getInt(int slot, String fieldName) {
        return transaction.getInt(block, offset(slot) + layout.offset(fieldName));
    }

    public String getString(int slot, String fieldName) {
        return transaction.getString(block, offset(slot) + layout.offset(fieldName));
    }

    public void setInt(int slot, String fieldName, int value) {
        transaction.setInt(block, offset(slot) + layout.offset(fieldName), value, true);
    }

    public void setString(int slot, String fieldName, String value) {
        transaction.setString(block, offset(slot) + layout.offset(fieldName), value, true);
    }

    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    public void format(){
        int slot = 0;
        while(isValidSlot(slot)){
            transaction.setInt(block, offset(slot), EMPTY, false);
            Schema schema = layout.schema();
            for(String fieldName: schema.fields()){
                int fieldPos = offset(slot) + layout.offset(fieldName);
                if(schema.type(fieldName) == java.sql.Types.INTEGER){
                    transaction.setInt(block, fieldPos, 0, false);
                }else{
                    transaction.setString(block, fieldPos, "", false);
                }
            }
            slot++;
        }
    }

    public int nextAfter(int slot){
        return searchAfter(slot, INUSE);
    }

    public int insertAfter(int slot){
        int newSlot = searchAfter(slot, EMPTY);
        if(newSlot >= 0){
            setFlag(newSlot, INUSE);
        }
        return newSlot;
    }

    public BlockId block() {
        return block;
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while(isValidSlot(slot)){
            if(transaction.getInt(block, offset(slot)) == flag){
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot+1) <= transaction.blockSize();
    }

    private void setFlag(int slot, int flag) {
        transaction.setInt(block, offset(slot), flag, true);
    }

    private int offset(int slot) {
        return slot * layout.slotSize();
    }
}
