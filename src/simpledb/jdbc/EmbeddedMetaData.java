package simpledb.jdbc;

import lombok.AllArgsConstructor;
import simpledb.record.Schema;

import java.sql.ResultSetMetaData;

import static java.sql.Types.INTEGER;

@AllArgsConstructor
public class EmbeddedMetaData extends ResultSetMetaDataAdapter {
    private Schema schema;

    public int getColumnCount(){
        return schema.fields().size();
    }

    public String getColumnName(int column){
        return schema.fields().get(column - 1);
    }

    public int getColumnType(int column){
        return schema.type(getColumnName(column));
    }

    public int getColumnDisplaySize(int column){
        String fieldName = getColumnName(column);
        int fieldType = schema.type(fieldName);
        int fieldLength = (fieldType == INTEGER) ? 6 : schema.length(fieldName);
        return Math.max(fieldLength, fieldName.length()) + 1;
    }
}
