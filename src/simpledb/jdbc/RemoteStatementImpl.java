package simpledb.jdbc;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.transaction.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
    private RemoteConnectionImpl remoteConnection;
    private Planner planner;

    public RemoteStatementImpl(RemoteConnectionImpl remoteConnection, Planner planner) throws RemoteException{
        this.remoteConnection = remoteConnection;
        this.planner = planner;
    }

    @Override
    public RemoteResultSet executeQuery(String query) throws RemoteException {
        try {
            Transaction transaction = remoteConnection.getTransaction();
            Plan plan = planner.createQueryPlan(query, transaction);
            return new RemoteResultSetImpl(plan, remoteConnection);
        } catch (RuntimeException e) {
            remoteConnection.rollback();
            throw e;
        }
    }

    @Override
    public int executeUpdate(String qry) throws RemoteException {
        try {
            Transaction transaction = remoteConnection.getTransaction();
            int result = planner.executeUpdate(qry, transaction);
            remoteConnection.commit();
            return result;
        } catch (RuntimeException e) {
            remoteConnection.rollback();
            throw e;
        }
    }

    @Override
    public void close() throws RemoteException {

    }
}
