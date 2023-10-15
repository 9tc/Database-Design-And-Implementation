package simpledb.jdbc;

import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;

public class RemoteMetaDataImpl extends UnicastRemoteObject implements RemoteMetaData {
    private Schema schema;
    private List<String> fields = new ArrayList<>();
    public RemoteMetaDataImpl(Schema schema) throws RemoteException{
        this.schema = schema;
        for (String fieldName : schema.fields()) {
            fields.add(fieldName);
        }
    }

    @Override
    public int getColumnCount() throws RemoteException {
        return fields.size();
    }

    @Override
    public String getColumnName(int column) throws RemoteException {
        return fields.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        return schema.type(fieldName);
    }

    @Override
    public int getColumnDisplaySize(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        int fieldType = schema.type(fieldName);
        int fieldLength = (fieldType == INTEGER) ? 6 : schema.length(fieldName);
        return Math.max(fieldLength, fieldName.length()) + 1;
    }
}
