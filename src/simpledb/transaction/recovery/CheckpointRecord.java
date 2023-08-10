package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class CheckpointRecord implements LogRecord{
    public CheckpointRecord() {

    }

    public static int writeToLog(LogManager lm) {
        byte[] record = new byte[Integer.BYTES];
        Page page = new Page(record);
        page.setInt(0, CHECKPOINT);
        return lm.append(record);
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    @Override
    public int transactionNumber() {
        return -1;
    }

    @Override
    public void undo(Transaction transaction) {
        // do nothing
    }

    public String toString() {
        return "<CHECKPOINT>";
    }
}
