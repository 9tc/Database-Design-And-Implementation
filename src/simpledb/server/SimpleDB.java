package simpledb.server;

import simpledb.buffer.BufferManager;
import simpledb.file.FileManager;
import simpledb.log.LogManager;

import java.io.File;

public class SimpleDB {
    public static String LOG_FILE = "simpledb.log";
    private final FileManager fm;
    private final LogManager lm;
    private final BufferManager bm;


    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileManager(dbDirectory, blocksize);
        lm = new LogManager(fm, LOG_FILE);
        bm = new BufferManager(fm, lm, buffsize);
    }

    public FileManager fileManager() {
        return fm;
    }

    public LogManager logManager() {
        return lm;
    }

    public BufferManager bufferManager() {
        return bm;
    }
}
