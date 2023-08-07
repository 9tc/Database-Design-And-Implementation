package simpledb.server;

import simpledb.file.FileManager;

import java.io.File;

public class SimpleDB {
    private final FileManager fm;

    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileManager(dbDirectory, blocksize);
//        lm = new LogMgr(fm, LOG_FILE);
//        bm = new BufferMgr(fm, lm, buffsize);
    }

    public FileManager fileManager() {
        return fm;
    }
}
