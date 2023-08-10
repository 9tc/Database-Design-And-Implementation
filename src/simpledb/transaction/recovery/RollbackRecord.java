package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class RollbackRecord implements LogRecord{
    private int transactionNumber;

    public RollbackRecord(Page page){
        int tpos = Integer.BYTES;
        transactionNumber = page.getInt(tpos);
    }


    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    @Override
    public void undo(Transaction transaction) {
        // do nothing
    }

    public static int writeToLog(LogManager lm, int transactionNumber) {
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setInt(0, ROLLBACK);
        page.setInt(Integer.BYTES, transactionNumber);
        return lm.append(record);
    }
}
