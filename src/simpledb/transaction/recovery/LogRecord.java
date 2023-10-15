package simpledb.transaction.recovery;

import simpledb.file.Page;
import simpledb.transaction.Transaction;

public interface LogRecord {
    int CHECKPOINT = 0;
    int START = 1;
    int COMMIT = 2;
    int ROLLBACK = 3;
    int SETINT = 4;
    int SETSTRING = 5;

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
