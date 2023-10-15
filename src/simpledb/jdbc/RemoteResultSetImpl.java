package simpledb.jdbc;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteResultSetImpl extends UnicastRemoteObject implements RemoteResultSet {
    private Scan scan;
    private Schema schema;
    private RemoteConnectionImpl remoteConnection;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl remoteConnection) throws RemoteException{
        scan = plan.open();
        schema = plan.getSchema();
        this.remoteConnection = remoteConnection;
    }

    @Override
    public boolean next() throws RemoteException {
        try{
            return scan.next();
        }catch (RuntimeException e) {
            remoteConnection.rollback();
            throw e;
        }
    }

    @Override
    public int getInt(String fieldName) throws RemoteException {
        try{
            fieldName = fieldName.toLowerCase();
            return scan.getInt(fieldName);
        }catch (RuntimeException e) {
            remoteConnection.rollback();
            throw e;
        }
    }

    @Override
    public String getString(String fieldName) throws RemoteException {
        try{
            fieldName = fieldName.toLowerCase();
            return scan.getString(fieldName);
        }catch (RuntimeException e) {
            remoteConnection.rollback();
            throw e;
        }
    }

    @Override
    public RemoteMetaData getMetaData() throws RemoteException {
        return new RemoteMetaDataImpl(schema);
    }

    @Override
    public void close() throws RemoteException {
        scan.close();
        remoteConnection.commit();
    }
}
