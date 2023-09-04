package simpledb.query;

import simpledb.record.RID;

public class SelectScan implements Scan {
    private final Scan s;
    private final Predicate pred;

    public SelectScan(Scan s, Predicate pred){
        this.s = s;
        this.pred = pred;
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        while (s.next()){
            if (pred.isSatisfied(s)){
                return true;
            }
        }
        return false;
    }

    @Override
    public int getInt(String fieldName) {
        return s.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return s.getString(fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        return s.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return s.hasField(fieldName);
    }

    @Override
    public void close() {
        s.close();
    }

    public void setInt(String fieldName, int value){
        UpdateScan us = (UpdateScan) s;
        us.setInt(fieldName, value);
    }

    public void setString(String fieldName, String value){
        UpdateScan us = (UpdateScan) s;
        us.setString(fieldName, value);
    }

    public void setValue(String fieldName, Constant value){
        UpdateScan us = (UpdateScan) s;
        us.setValue(fieldName, value);
    }

    public void delete(){
        UpdateScan us = (UpdateScan) s;
        us.delete();
    }

    public void insert(){
        UpdateScan us = (UpdateScan) s;
        us.insert();
    }

    public RID getRid(){
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
    }

    public void moveToRid(RID rid){
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(rid);
    }
}
