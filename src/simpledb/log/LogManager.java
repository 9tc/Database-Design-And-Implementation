package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;

import java.util.Iterator;

/**
 * @ClassName LogManager
 * @Description ログを適切に遅延しながらファイルに書き出す役割 ディスクアクセスを抑制するため
 */

public class LogManager {

    private BlockId currentBlock;
    private final FileManager fm;
    private final String logFile;
    private final Page logPage;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fm, String logFile) {
        this.fm = fm;
        this.logFile = logFile;
        byte[] b = new byte[fm.blockSize()];
        logPage = new Page(b);
        int logSize = fm.length(logFile);
        if(logSize == 0){
            currentBlock = appendNewBlock();
        }else{
            currentBlock = new BlockId(logFile, logSize - 1);
            fm.read(currentBlock, logPage);
        }
    }

    public void flush(int lsn){
        if(lsn >= lastSavedLSN) flush();
    }

    private void flush() {
        fm.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }


    private BlockId appendNewBlock() {
        BlockId block = fm.append(logFile);
        logPage.setInt(0, fm.blockSize());
        fm.write(block, logPage);
        return block;
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentBlock);
    }


    public synchronized int append(byte[] record) {
        int boundary = logPage.getInt(0);
        int recordSize = record.length;
        int bytesNeeded = recordSize + Integer.BYTES;
        if(boundary - bytesNeeded < Integer.BYTES){
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }
        int recpos = boundary - bytesNeeded;
        logPage.setBytes(recpos, record);
        logPage.setInt(0, recpos);
        latestLSN++;
        return latestLSN;
    }
}
