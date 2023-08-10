package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;
import simpledb.log.LogManager;

public class Buffer {

    private final FileManager fm;
    private final LogManager lm;
    private final Page contents;
    private BlockId block = null;
    private int txnum = -1;
    private int lsn = -1;
    private int pins = 0;

    public Buffer(FileManager fm, LogManager lm) {
        this.fm = fm;
        this.lm = lm;
        contents = new Page(fm.blockSize());
    }
    public Page contents() {
        return contents;
    }

    public BlockId block(){
        return block;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if(lsn >= 0) this.lsn = lsn;
    }

    public boolean isPinned(){
        return pins > 0;
    }

    public int modifyingTx(){
        return txnum;
    }

    public void assignToBlock(BlockId blk) {
        flush();
        block = blk;
        fm.read(blk, contents);
        pins = 0;
    }

    public void flush() {
        if(txnum >= 0){
            lm.flush(lsn);
            fm.write(block, contents);
            txnum = -1;
        }
    }

    void pin(){
        pins++;
    }

    void unpin(){
        pins--;
    }

    public boolean isModifiedBy(int txnum) {
        return this.txnum == txnum;
    }
}
