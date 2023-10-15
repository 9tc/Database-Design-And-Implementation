package simpledb.plan;

import lombok.AllArgsConstructor;
import simpledb.metadata.MetadataManager;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class BetterQueryPlanner implements QueryPlanner{
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
            Plan p1 = new ProductPlan(plan, nextPlan);
            Plan p2 = new ProductPlan(nextPlan, plan);
            plan = p1.blocksAccessed() < p2.blocksAccessed() ? p1 : p2;
        }
        return new ProjectPlan(new SelectPlan(plan, data.getPredicate()), data.getFields());
    }
}
