package simpledb.transaction.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class SetIntRecord implements LogRecord {
    private final int transactionNumber;
    private final int offset;
    private final int value;
    private final BlockId block;

    public SetIntRecord(Page page) {
        int tpos = Integer.BYTES;
        transactionNumber = page.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = page.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blockNum = page.getInt(bpos);
        block = new BlockId(filename, blockNum);
        int opos = bpos + Integer.BYTES;
        offset = page.getInt(opos);
        int vpos = opos + Integer.BYTES;
        value = page.getInt(vpos);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    public String toString(){
        return "<SETINT " + transactionNumber + " " + block + " " + offset + " " + value + ">";
    }

    @Override
    public void undo(Transaction transaction) {
        transaction.pin(block);
        transaction.setInt(block, offset, value, false);
        transaction.unpin(block);
    }

    public static int writeToLog(LogManager lm, int transactionNumber, BlockId block, int offset, int oldValue) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(block.getFilename().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Integer.BYTES;
        byte[] record = new byte[reclen];
        Page page = new Page(record);
        page.setInt(0, SETINT);
        page.setInt(tpos, transactionNumber);
        page.setString(fpos, block.getFilename());
        page.setInt(bpos, block.getBlockNum());
        page.setInt(opos, offset);
        page.setInt(vpos, oldValue);
        return lm.append(record);
    }
}
