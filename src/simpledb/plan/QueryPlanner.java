package simpledb.plan;

import simpledb.parse.QueryData;
import simpledb.transaction.Transaction;

public interface QueryPlanner {
    public Plan createPlan(QueryData data, Transaction transaction);
}
