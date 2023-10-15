package simpledb.plan;

import lombok.AllArgsConstructor;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.SelectScan;
import simpledb.record.Schema;

@AllArgsConstructor
public class SelectPlan implements Plan{
    private final Plan plan;
    private final Predicate predicate;

    @Override
    public Scan open() {
        return new SelectScan(plan.open(), predicate);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput() / predicate.reductionFactor(plan);
    }

    @Override
    public int distinctValues(String fieldName) {
        if(predicate.equatesWithConstant(fieldName) != null){
            return 1;
        }else{
            String fieldName2 = predicate.equatesWithField(fieldName);
            if(fieldName2 != null){
                return Math.min(plan.distinctValues(fieldName), plan.distinctValues(fieldName2));
            }else{
                return plan.distinctValues(fieldName);
            }
        }
    }

    @Override
    public Schema getSchema() {
        return plan.getSchema();
    }
}
