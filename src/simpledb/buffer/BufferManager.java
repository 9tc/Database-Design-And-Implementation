package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.log.LogManager;

public class BufferManager {
    private static final long MAX_TIME_MS = 10 * 1000;
    private final Buffer[] bufferpool;
    private int numAvailable;

    public BufferManager(FileManager fm, LogManager lm, int buffsize) {
        bufferpool = new Buffer[buffsize];
        numAvailable = buffsize;
        for(int i = 0; i < numAvailable; ++i){
            bufferpool[i] = new Buffer(fm, lm);
        }
    }

    public synchronized void flushAll(int txnum) {
        for(Buffer buff: bufferpool){
            if(buff.isModifiedBy(txnum)){
                buff.flush();
            }
        }
    }

    public synchronized Buffer pin(BlockId block) {
        try{
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(block);
            while(buff == null && !waitingTooLong(timestamp)){
                wait(MAX_TIME_MS);
                buff = tryToPin(block);
            }
            if(buff == null){
                throw new BufferAbortException();
            }
            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    private boolean waitingTooLong(long startTimestamp){
        return System.currentTimeMillis() - startTimestamp > MAX_TIME_MS;
    }

    private Buffer tryToPin(BlockId block) {
        Buffer buff = findExistingBuffer(block);
        if(buff == null){
            buff = chooseUnpinnedBuffer();
            if(buff == null){
                return null;
            }
            buff.assignToBlock(block);
        }
        if(!buff.isPinned()){
            numAvailable--;
        }
        buff.pin();
        return buff;
    }

    private Buffer chooseUnpinnedBuffer() {
        for(Buffer buff: bufferpool){
            if(!buff.isPinned()){
                return buff;
            }
        }
        return null;
    }

    private Buffer findExistingBuffer(BlockId block) {
        for(Buffer buff: bufferpool){
            BlockId b = buff.block();
            if(b != null && b.equals(block)){
                return buff;
            }
        }
        return null;
    }

    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if(!buff.isPinned()){
            numAvailable++;
            notifyAll();
        }
    }

    public synchronized int available() {
        return numAvailable;
    }
}
