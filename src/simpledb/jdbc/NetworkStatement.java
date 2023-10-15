package simpledb.jdbc;

import lombok.AllArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@AllArgsConstructor
public class NetworkStatement extends StatementAdapter {
    private RemoteStatement remoteStatement;

    public ResultSet executeQuery(String qry) throws SQLException {
        try{
            RemoteResultSet remoteResultSet = remoteStatement.executeQuery(qry);
            return new NetworkResultSet(remoteResultSet);
        }catch(Exception e){
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException {
        try{
            remoteStatement.close();
        }catch(Exception e){
            throw new SQLException(e);
        }
    }
}
