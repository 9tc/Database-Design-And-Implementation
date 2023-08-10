package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.transaction.Transaction;

public interface LogRecord {
    static final int CHECKPOINT = 0;
    static final int START = 1;
    static final int COMMIT = 2;
    static final int ROLLBACK = 3;
    static final int SETINT = 4;
    static final int SETSTRING = 5;

    int op();
    int transactionNumber();
    void undo(Transaction transaction);

    static LogRecord create(byte[] bytes){
        Page page = new Page(bytes);
        switch (page.getInt(0)){
            case CHECKPOINT -> {
                return new CheckpointRecord();
            }
            case START -> {
                return new StartRecord(page);
            }
            case COMMIT -> {
                return new CommitRecord(page);
            }
            case ROLLBACK -> {
                return new RollbackRecord(page);
            }
            case SETINT -> {
                return new SetIntRecord(page);
            }
            case SETSTRING -> {
                return new SetStringRecord(page);
            }
            default -> {
                return null;
            }
        }
    }
}
