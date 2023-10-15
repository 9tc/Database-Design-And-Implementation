package simpledb.jdbc;

import java.rmi.RemoteException;

public interface RemoteDriver {
    RemoteConnection connect() throws RemoteException;
}
