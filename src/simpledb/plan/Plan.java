package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public interface Plan {
    public Scan open();
    public int blocksAccessed();
    public int recordsOutput();
    public int distinctValues(String fieldName);
    public Schema getSchema();
}
