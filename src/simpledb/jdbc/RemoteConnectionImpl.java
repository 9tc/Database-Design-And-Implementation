package simpledb.jdbc;

import lombok.Getter;
import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.transaction.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
    private final SimpleDB db;
    @Getter
    private Transaction transaction;
    private final Planner planner;

    RemoteConnectionImpl(SimpleDB db) throws RemoteException {
        this.db = db;
        transaction = db.newTransaction();
        planner = db.planner();
    }

    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this, planner);
    }

    public void close() throws RemoteException {
        transaction.commit();
    }

    void commit(){
        transaction.commit();
        transaction = db.newTransaction();
    }

    void rollback(){
        transaction.rollback();
        transaction = db.newTransaction();
    }
}
