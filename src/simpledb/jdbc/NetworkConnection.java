package simpledb.jdbc;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@AllArgsConstructor
public class NetworkConnection extends ConnectionAdapter {
    private RemoteConnection remoteConnection;

    public Statement createStatement() {
        try{
            RemoteStatement remoteStatement = remoteConnection.createStatement();
            return new NetworkStatement(remoteStatement);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public void close() throws SQLException {
        try{
            remoteConnection.close();
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
}
