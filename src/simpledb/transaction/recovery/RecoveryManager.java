package simpledb.transaction.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.file.BlockId;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class RecoveryManager {
    private LogManager lm;
    private BufferManager bm;
    private Transaction transaction;
    private int transactionNumber;

    public RecoveryManager(Transaction transaction, int transactionNumber, LogManager lm, BufferManager bm) {
        this.transaction = transaction;
        this.transactionNumber = transactionNumber;
        this.lm = lm;
        this.bm = bm;
        StartRecord.writeToLog(lm, transactionNumber);
    }

    public void commit(){
        bm.flushAll(transactionNumber);
        int lsn = CommitRecord.writeToLog(lm, transactionNumber);
        lm.flush(lsn);
    }

    public void rollback(){
        doRollback();
        bm.flushAll(transactionNumber);
        int lsn = RollbackRecord.writeToLog(lm, transactionNumber);
        lm.flush(lsn);
    }

    public void recover(){
        doRecover();
        bm.flushAll(transactionNumber);
        int lsn = CheckpointRecord.writeToLog(lm);
        lm.flush(lsn);
    }

    public int setInt(Buffer buff, int offset, int newValue){
        int oldValue = buff.contents().getInt(offset);
        BlockId block = buff.block();
        return SetIntRecord.writeToLog(lm, transactionNumber, block, offset, oldValue);
    }

    public int setString(Buffer buff, int offset, String newValue){
        String oldValue = buff.contents().getString(offset);
        BlockId block = buff.block();
        return SetStringRecord.writeToLog(lm, transactionNumber, block, offset, oldValue);
    }

    private void doRollback(){
        Iterator<byte[]> it = lm.iterator();

        while(it.hasNext()){
            byte[] bytes = it.next();
            LogRecord record = LogRecord.create(bytes);
            if(record.transactionNumber() == transactionNumber){
                if(record.op() == LogRecord.START) return;
                record.undo(transaction);
            }
        }
    }

    private void doRecover(){
        Collection<Integer> finishedTransactions = new ArrayList<Integer>();
        Iterator<byte[]> it = lm.iterator();
        while(it.hasNext()){
            byte[] bytes = it.next();
            LogRecord record = LogRecord.create(bytes);
            if(record.op() == LogRecord.CHECKPOINT) return;
            if(record.op() == LogRecord.COMMIT || record.op() == LogRecord.ROLLBACK){
                finishedTransactions.add(record.transactionNumber());
            } else if(!finishedTransactions.contains(record.transactionNumber())){
                record.undo(transaction);
            }
        }
    }
}
