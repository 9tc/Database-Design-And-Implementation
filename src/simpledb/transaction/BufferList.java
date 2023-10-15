package simpledb.transaction;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
    private final Map<BlockId, Buffer> buffers = new HashMap<>();
    private final List<BlockId> pins = new ArrayList<>();
    private final BufferManager bm;

    public BufferList(BufferManager bm) {
        this.bm = bm;
    }

    public Buffer getBuffer(BlockId block) {
        return buffers.get(block);
    }

    public void pin(BlockId block) {
        Buffer buff = bm.pin(block);
        buffers.put(block, buff);
        pins.add(block);
    }

    public void unpin(BlockId block) {
        Buffer buff = buffers.get(block);
        bm.unpin(buff);
        pins.remove(block);
        if (!pins.contains(block)) {
            buffers.remove(block);
        }
    }

    public void unpinAll(){
        for(BlockId block: pins){
            Buffer buff = buffers.get(block);
            bm.unpin(buff);
        }
        pins.clear();
        buffers.clear();
    }
}
