package simpledb.plan;

import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan{
    private final Plan plan;
    private Schema schema = new Schema();

    public ProjectPlan(Plan plan, List<String> fieldList){
        this.plan = plan;
        for (String fieldName : fieldList){
            schema.add(fieldName, plan.getSchema());
        }
    }

    @Override
    public Scan open() {
        return new ProjectScan(plan.open(), schema.fields());
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
