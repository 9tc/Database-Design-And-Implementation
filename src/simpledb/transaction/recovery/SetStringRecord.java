package simpledb.transaction.recovery;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.transaction.Transaction;

public class SetStringRecord implements LogRecord{
    private int transactionNumber, offset;
    private String value;
    private BlockId block;

    public SetStringRecord(Page page) {
        int tpos = Integer.BYTES;
        transactionNumber = page.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = page.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blocknum = page.getInt(bpos);
        block = new BlockId(filename, blocknum);
        int opos = bpos + Integer.BYTES;
        offset = page.getInt(opos);
        int vpos = opos + Integer.BYTES;
        value = page.getString(vpos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int transactionNumber() {
        return transactionNumber;
    }

    public String toString(){
        return "<SETSTRING " + transactionNumber + " " + block + " " + offset + " " + value + ">";
    }

    @Override
    public void undo(Transaction transaction){
        transaction.pin(block);
        transaction.setString(block, offset, value, false);
        transaction.unpin(block);
    }

    public static int writeToLog(LogManager lm, int transactionnum, BlockId block, int offset, String value){
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(block.fileName().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(value.length());
        byte[] record = new byte[reclen];
        Page page = new Page(record);
        page.setInt(0, SETSTRING);
        page.setInt(tpos, transactionnum);
        page.setString(fpos, block.fileName());
        page.setInt(bpos, block.number());
        page.setInt(opos, offset);
        page.setString(vpos, value);
        return lm.append(record);
    }
}
