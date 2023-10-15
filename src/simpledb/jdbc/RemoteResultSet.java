package simpledb.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteResultSet extends Remote {
    boolean next() throws RemoteException;
    int getInt(String fieldName) throws RemoteException;
    String getString(String fieldName) throws RemoteException;
    RemoteMetaData getMetaData() throws RemoteException;
    void close() throws RemoteException;
}
