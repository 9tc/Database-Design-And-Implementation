package simpledb.transaction.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static final LockTable lockTable = new LockTable();
    private final Map<BlockId, String> locks = new HashMap<>();

    public void sLock(BlockId block){
        if(!locks.containsKey(block)){
            lockTable.sLock(block);
            locks.put(block, "S");
        }
    }

    public void xLock(BlockId block){
        if(!hasXLock(block)){
            sLock(block);
            lockTable.xLock(block);
            locks.put(block, "X");
        }
    }

    public void release() {
        for(BlockId block: locks.keySet()){
            lockTable.unlock(block);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId block){
        return locks.containsKey(block) && locks.get(block).equals("X");
    }
}
