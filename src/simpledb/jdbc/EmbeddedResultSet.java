package simpledb.jdbc;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class EmbeddedResultSet extends ResultSetAdapter{
    private final Scan scan;
    private final Schema schema;
    private final EmbeddedConnection connection;

    public EmbeddedResultSet(Plan plan, EmbeddedConnection connection){
        scan = plan.open();
        schema = plan.getSchema();
        this.connection = connection;
    }

    public boolean next() throws SQLException {
        try{
            return scan.next();
        }catch (RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public int getInt(String fieldName) throws SQLException{
        try{
            return scan.getInt(fieldName.toLowerCase());
        }catch (RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public String getString(String fieldName) throws SQLException{
        try{
            return scan.getString(fieldName.toLowerCase());
        }catch (RuntimeException e){
            connection.rollback();
            throw new SQLException(e);
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException{
        return new EmbeddedMetaData(schema);
    }

    public void close() throws SQLException{
        scan.close();
        connection.commit();
    }
}
