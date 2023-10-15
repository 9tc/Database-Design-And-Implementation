package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class CommitRecord implements LogRecord{
    private final int transactionNumber;

    public CommitRecord(Page page){
        int pos = Integer.BYTES;
        transactionNumber = page.getInt(pos);
    }

    public int op(){
        return COMMIT;
    }

    public int transactionNumber(){
        return transactionNumber;
    }

    public void undo(Transaction transaction){
        // do nothing
    }

    public String toString(){
        return "<COMMIT " + transactionNumber + ">";
    }

    public static int writeToLog(LogManager lm, int transactionNumber){
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setInt(0, COMMIT);
        page.setInt(Integer.BYTES, transactionNumber);
        return lm.append(record);
    }
}
