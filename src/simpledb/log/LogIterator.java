package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
    private final FileManager fm;
    private BlockId blockId;
    private final Page page;
    private int currentPos;

    public LogIterator(FileManager fm, BlockId currentBlock) {
        this.fm = fm;
        this.blockId = currentBlock;
        byte[] b = new byte[fm.blockSize()];
        page = new Page(b);

        moveToBlock(blockId);
    }

    @Override
    public boolean hasNext() {
        return currentPos < fm.blockSize() || blockId.getBlockNum() > 0;
    }

    @Override
    public byte[] next() {
        if(currentPos == fm.blockSize()){
            blockId = new BlockId(blockId.getFilename(), blockId.getBlockNum() - 1);
            moveToBlock(blockId);
        }

        byte[] record = page.getBytes(currentPos);
        currentPos += Integer.BYTES + record.length;
        return record;
    }

    private void moveToBlock(BlockId block) {
        fm.read(block, page);
        int boundary = page.getInt(0);
        currentPos = boundary;
    }
}
