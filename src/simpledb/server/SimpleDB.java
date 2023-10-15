package simpledb.server;

import simpledb.buffer.BufferManager;
import simpledb.file.FileManager;
import simpledb.log.LogManager;
import simpledb.metadata.MetadataManager;
import simpledb.plan.BasicQueryPlanner;
import simpledb.plan.BasicUpdatePlanner;
import simpledb.plan.Planner;
import simpledb.transaction.Transaction;

import java.io.File;

public class SimpleDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";
    private final FileManager fm;
    private final LogManager lm;
    private final BufferManager bm;
    private Planner planner;
    private MetadataManager mdm;


    public SimpleDB(String dirname, int blockSize, int buffSize) {
        File dbDirectory = new File(dirname);
        fm = new FileManager(dbDirectory, blockSize);
        lm = new LogManager(fm, LOG_FILE);
        bm = new BufferManager(fm, lm, buffSize);
    }

    public SimpleDB(String dirname){
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        Transaction transaction = newTransaction();
        boolean isNew = fm.isNew();
        if(isNew){
            System.out.println("creating new database");
        }else{
            System.out.println("recovering existing database");
            transaction.recover();
        }
        mdm = new MetadataManager(isNew, transaction);
        planner = new Planner(new BasicQueryPlanner(mdm), new BasicUpdatePlanner(mdm));
        transaction.commit();
        System.out.println("database ready");
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

    public Transaction newTransaction() {
        return new Transaction(fm, lm, bm);
    }

    public MetadataManager metadataManager() {
        return mdm;
    }

    public Planner planner() {
        return planner;
    }
}
