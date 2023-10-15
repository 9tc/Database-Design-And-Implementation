package simpledb.transaction.concurrency;

import simpledb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    private static final long MAX_TIME_MS = 10000;
    private final Map<BlockId, Integer> locks = new HashMap<>();

    public synchronized void sLock(BlockId block){
        try{
            long timestamp = System.currentTimeMillis();
            while(hasXLock(block) && !waitingTooLong(timestamp)){
                wait(MAX_TIME_MS);
            }
            if(hasXLock(block)){
                throw new LockAbortException();
            }

            int value = getLockValue(block);
            locks.put(block, value + 1);
        }catch (InterruptedException e){
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(BlockId block){
        try{
            long timestamp = System.currentTimeMillis();
            while(hasOtherSLocks(block) && !waitingTooLong(timestamp)){
                wait(MAX_TIME_MS);
            }
            if(hasOtherSLocks(block)){
                throw new LockAbortException();
            }
            locks.put(block, -1);
        }catch (InterruptedException e){
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(BlockId block){
        int value = getLockValue(block);
        if (value > 1){
            locks.put(block, value - 1);
        }else{
            locks.remove(block);
            notifyAll();
        }
    }

    private boolean hasXLock(BlockId block){
        return getLockValue(block) < 0;
    }

    private boolean hasOtherSLocks(BlockId block){
        return getLockValue(block) > 1;
    }

    private boolean waitingTooLong(long startTime){
        return System.currentTimeMillis() - startTime > MAX_TIME_MS;
    }

    private int getLockValue(BlockId block){
        Integer value = locks.get(block);
        return (locks.get(block) == null) ? 0 : value;
    }
}
