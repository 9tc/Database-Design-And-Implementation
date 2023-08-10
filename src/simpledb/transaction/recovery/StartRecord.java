package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class StartRecord implements LogRecord{
    private int transactionnum;

    public StartRecord(Page page){
        int tpos = Integer.BYTES;
        transactionnum = page.getInt(tpos);
    }

    public int op(){
        return START;
    }

    public int transactionNumber(){
        return transactionnum;
    }

    public void undo(Transaction transaction){
        // do nothing
    }

    public String toString(){
        return "<START " + transactionnum + ">";
    }

    public static int writeToLog(LogManager lm, int transactionnum){
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setInt(0, START);
        page.setInt(Integer.BYTES, transactionnum);
        return lm.append(record);
    }
}
