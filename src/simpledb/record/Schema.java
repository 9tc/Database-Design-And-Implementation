package simpledb.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
    private List<String> fields = new ArrayList<String>();
    private Map<String, FieldInfo> info = new HashMap<String, FieldInfo>();
    // TODO fieldType -> FieldType型にしたい
    public void addField(String fieldName, int fieldType, int fieldLength) {
        fields.add(fieldName);
        info.put(fieldName, new FieldInfo(fieldType, fieldLength));
    }

    public void addIntField(String fieldName) {
        addField(fieldName, java.sql.Types.INTEGER, 0);
    }

    public void addStringField(String fieldName, int fieldLength) {
        addField(fieldName, java.sql.Types.VARCHAR, fieldLength);
    }

    public void add(String fieldName, Schema schema){
        addField(fieldName, schema.type(fieldName), schema.length(fieldName));
    }

    public void addAll(Schema schema){
        for(String fieldName : schema.fields()){
            add(fieldName, schema);
        }
    }

    public List<String> fields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public int type(String fieldName) {
        return info.get(fieldName).type;
    }

    public int length(String fieldName) {
        return info.get(fieldName).length;
    }

    class FieldInfo{
        int type, length;
        public FieldInfo(int type, int length) {
            this.type = type;
            this.length = length;
        }
    }
}
