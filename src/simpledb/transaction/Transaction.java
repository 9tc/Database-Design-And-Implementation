package simpledb.transaction;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.concurrency.ConcurrencyManager;
import simpledb.transaction.recovery.RecoveryManager;

public class Transaction {
    private static int nextTransactionNumber = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryManager rm;
    private ConcurrencyManager cm;
    private BufferManager bm;
    private FileManager fm;
    private int transactionNumber;
    private BufferList myBuffers;
    public Transaction(FileManager fm, LogManager lm, BufferManager bm) {
        this.fm = fm;
        this.bm = bm;
        this.transactionNumber = nextTransactionNumber();
        this.rm = new RecoveryManager(this, transactionNumber, lm, bm);
        this.cm = new ConcurrencyManager();
        this.myBuffers = new BufferList(bm);

    }

    public void pin(BlockId block) {
        myBuffers.pin(block);
    }

    public void setInt(BlockId block, int offset, int value, boolean logging) {
        cm.sLock(block);
        Buffer buff = myBuffers.getBuffer(block);
        int lsn = -1;
        if (logging) {
            lsn = rm.setInt(buff, offset, value);
        }
        Page page = buff.contents();
        page.setInt(offset, value);
        buff.setModified(transactionNumber, lsn);
    }

    public void setString(BlockId block, int offset, String value, boolean logging) {
        cm.sLock(block);
        Buffer buff = myBuffers.getBuffer(block);
        int lsn = -1;
        if (logging) {
            lsn = rm.setString(buff, offset, value);
        }
        Page page = buff.contents();
        page.setString(offset, value);
        buff.setModified(transactionNumber, lsn);
    }

    public void commit() {
        rm.commit();
        cm.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + transactionNumber + " committed");
    }

    public int getInt(BlockId block, int offset) {
        cm.sLock(block);
        Buffer buff = myBuffers.getBuffer(block);
        return buff.contents().getInt(offset);
    }

    public String getString(BlockId block, int offset) {
        cm.sLock(block);
        Buffer buff = myBuffers.getBuffer(block);
        return buff.contents().getString(offset);
    }

    public void rollback() {
        rm.rollback();
        cm.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + transactionNumber + " rolled back");
    }

    public void recover(){
        bm.flushAll(transactionNumber);
        rm.recover();
    }

    public void unpin(BlockId block) {
        myBuffers.unpin(block);
    }

    public int size(String filename){
        BlockId block = new BlockId(filename, END_OF_FILE);
        cm.sLock(block);
        return fm.length(filename);
    }

    public BlockId append(String filename){
        BlockId block = new BlockId(filename, END_OF_FILE);
        cm.xLock(block);
        return fm.append(filename);
    }

    public int blockSize(){
        return fm.blockSize();
    }

    public int availableBuffers(){
        return bm.available();
    }

    private static synchronized int nextTransactionNumber() {
        nextTransactionNumber++;
        System.out.println("new transaction: " + nextTransactionNumber);
        return nextTransactionNumber;
    }
}
