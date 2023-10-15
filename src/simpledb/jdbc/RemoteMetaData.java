package simpledb.jdbc;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMetaData extends Remote {
    int getColumnCount() throws RemoteException;
    String getColumnName(int column) throws RemoteException;
    int getColumnType(int column) throws RemoteException;
    int getColumnDisplaySize(int column) throws RemoteException;
}
