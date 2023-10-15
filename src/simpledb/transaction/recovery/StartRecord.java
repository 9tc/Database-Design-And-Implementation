package simpledb.transaction.recovery;

import lombok.ToString;
import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

@ToString
public class StartRecord implements LogRecord{
    private final int transactionNum;

    public StartRecord(Page page){
        int tpos = Integer.BYTES;
        transactionNum = page.getInt(tpos);
    }

    public int op(){
        return START;
    }

    public int transactionNumber(){
        return transactionNum;
    }

    public void undo(Transaction transaction){
        // do nothing
    }

    public static int writeToLog(LogManager lm, int transactionnum){
        byte[] record = new byte[Integer.BYTES * 2];
        Page page = new Page(record);
        page.setInt(0, START);
        page.setInt(Integer.BYTES, transactionnum);
        return lm.append(record);
    }
}
