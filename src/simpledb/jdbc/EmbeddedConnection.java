package simpledb.jdbc;

import lombok.Getter;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.transaction.Transaction;

import java.sql.SQLException;

public class EmbeddedConnection extends ConnectionAdapter{
    private final SimpleDB db;
    @Getter
    private Transaction transaction;
    private final Planner planner;

    public EmbeddedConnection(SimpleDB db){
        this.db = db;
        transaction = db.newTransaction();
        planner = db.planner();
    }

    public EmbeddedStatement createStatement() throws SQLException {
        return new EmbeddedStatement(this, planner);
    }

    public void commit() throws SQLException {
        transaction.commit();
        transaction = db.newTransaction();
    }

    public void close() throws SQLException {
        transaction.commit();
        transaction = db.newTransaction();
    }

    public void rollback() throws SQLException {
        transaction.rollback();
        transaction = db.newTransaction();
    }
}
