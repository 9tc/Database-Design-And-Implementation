package simpledb.jdbc;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class NetworkDriver extends DriverAdapter{
    public Connection connect(String url, Properties properties) throws SQLException{
        try{
            String host = url.replace("jdbc:simpledb://", "");
            Registry reg = LocateRegistry.getRegistry(host, 1099);
            RemoteDriver remoteDriver = (RemoteDriver) reg.lookup("simpledb");
            RemoteConnection remoteConnection = remoteDriver.connect();
            return new NetworkConnection(remoteConnection);
        } catch (RemoteException | NotBoundException e) {
           throw new SQLException(e);
        }
    }
}
