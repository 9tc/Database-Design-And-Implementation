package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private final FileManager fm;
    private final BlockId blockId;
    private final Page page;
    private int boundary;
    private int currentpos;

    public LogIterator(FileManager fm, BlockId currentBlock) {
        this.fm = fm;
        this.blockId = currentBlock;
        byte[] b = new byte[fm.blockSize()];
        page = new Page(b);

        moveToBlock(blockId);
    }

    private void moveToBlock(BlockId block) {
        fm.read(block, page);
        boundary = page.getInt(0);
        currentpos = boundary;
    }

    @Override
    public boolean hasNext() {
        return currentpos < fm.blockSize() || blockId.number() > 0;
    }

    @Override
    public byte[] next() {
        if(currentpos == fm.blockSize()){
            moveToBlock(new BlockId(blockId.fileName(), blockId.number() - 1));
        }

        byte[] record = page.getBytes(currentpos);
        currentpos += Integer.BYTES + record.length;
        return record;
    }
}
