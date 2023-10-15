package simpledb.plan;

import simpledb.query.ProductScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class ProductPlan implements Plan{
    private final Plan plan1;
    private final Plan plan2;
    private Schema schema = new Schema();

    public ProductPlan(Plan plan1, Plan plan2){
        this.plan1 = plan1;
        this.plan2 = plan2;
        schema.addAll(plan1.getSchema());
        schema.addAll(plan2.getSchema());
    }

    @Override
    public Scan open() {
        return new ProductScan(plan1.open(), plan2.open());
    }

    @Override
    public int blocksAccessed() {
        return plan1.blocksAccessed() + plan2.blocksAccessed() * plan1.recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return plan1.recordsOutput() * plan2.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if(plan1.getSchema().hasField(fieldName)){
            return plan1.distinctValues(fieldName);
        }else{
            return plan2.distinctValues(fieldName);
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
