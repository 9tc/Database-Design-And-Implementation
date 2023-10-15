package simpledb.jdbc;

import lombok.AllArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;

@AllArgsConstructor
public class NetworkResultSet extends ResultSetAdapter {
    private RemoteResultSet remoteResultSet;

    @Override
    public boolean next() throws SQLException {
        try{
            return remoteResultSet.next();
        }catch (Exception e){
            throw new SQLException(e);
        }
    }
}
