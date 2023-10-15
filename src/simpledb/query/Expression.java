package simpledb.query;

import simpledb.record.Schema;

public class Expression {
    private Constant val = null;
    private String fldname = null;

    public Expression(Constant val){
        this.val = val;
    }

    public Expression(String fieldName){
        this.fldname = fieldName;
    }

    public Constant evaluate(Scan s){
        if (val != null){
            return val;
        } else {
            return s.getValue(fldname);
        }
    }

    public Constant asConstant(){
        return val;
    }

    public String asFieldName(){
        return fldname;
    }

    public boolean appliesTo(Schema sch){
        if (val != null){
            return true;
        } else {
            return sch.hasField(fldname);
        }
    }

    public String toString(){
        if (val != null){
            return val.toString();
        } else {
            return fldname;
        }
    }

    public boolean isFieldName() {
        return fldname != null;
    }
}
