package simpledb.query;

import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class ProjectScan implements Scan{
    private final Scan s;
    private final List<String> fieldList;

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public int getInt(String fieldName) {
        if(hasField(fieldName)){
            return s.getInt(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public String getString(String fieldName) {
        if(hasField(fieldName)){
            return s.getString(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public Constant getValue(String fieldName) {
        if(hasField(fieldName)){
            return s.getValue(fieldName);
        } else {
            throw new RuntimeException("field " + fieldName + " not found.");
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        s.close();
    }
}
