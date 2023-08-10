package simpledb.query;

import simpledb.record.RID;

public interface UpdateScan extends Scan{
    public void setInt(String fieldName, int value);
    public void setString(String fieldName, String value);
    public void setValue(String fieldName, Constant value);
    public void delete();
    public void insert();

    public RID getRid();
    public void moveToRid(RID rid);
}
