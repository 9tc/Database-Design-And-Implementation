package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

public interface Index {
    public void beforeFirst(Constant searchKey);
    public boolean next();
    public RID getDataRid();
    public void insert(Constant dataVal, RID dataRid);
    public void delete(Constant dataVal, RID dataRid);
    public void close();
}
