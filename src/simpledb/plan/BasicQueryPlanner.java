package simpledb.plan;

import lombok.AllArgsConstructor;
import simpledb.metadata.MetadataManager;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class BasicQueryPlanner implements QueryPlanner{
    private MetadataManager metadataManager;

    @Override
    public Plan createPlan(QueryData data, Transaction transaction) {
        List<Plan> plans = new ArrayList<>();
        for(String tableName : data.getTables()){
            String viewDefinition = metadataManager.getViewDefinition(tableName, transaction);
            if(viewDefinition != null){
                plans.add(createPlan(new Parser(viewDefinition).query(), transaction));
            }else{
                plans.add(new TablePlan(transaction, tableName, metadataManager));
            }
        }

        Plan plan = plans.remove(0);
        for(Plan nextPlan : plans){
            plan = new ProductPlan(plan, nextPlan);
        }
        return new ProjectPlan(new SelectPlan(plan, data.getPredicate()), data.getFields());
    }
}
