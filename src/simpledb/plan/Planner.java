package simpledb.plan;

import lombok.AllArgsConstructor;
import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.Parser;
import simpledb.parse.UpdateData;
import simpledb.transaction.Transaction;

@AllArgsConstructor
public class Planner {
    private QueryPlanner queryPlanner;
    private UpdatePlanner updatePlanner;

    public Plan createQueryPlan(String query, Transaction transaction){
        return queryPlanner.createPlan(new Parser(query).query(), transaction);
    }

    public int executeUpdate(String query, Transaction transaction){
        Object obj = new Parser(query).updateCommand();
        if(obj instanceof InsertData) {
            return updatePlanner.executeInsert((InsertData) obj, transaction);
        }else if(obj instanceof DeleteData) {
            return updatePlanner.executeDelete((DeleteData) obj, transaction);
        }else if (obj instanceof UpdateData) {
            return updatePlanner.executeUpdate((UpdateData) obj, transaction);
        }else if(obj instanceof CreateTableData) {
            return updatePlanner.executeCreateTable((CreateTableData) obj, transaction);
        }else if(obj instanceof CreateViewData) {
            return updatePlanner.executeCreateView((CreateViewData) obj, transaction);
        }else if(obj instanceof CreateIndexData) {
            return updatePlanner.executeCreateIndex((CreateIndexData) obj, transaction);
        }else{
            return 0;
        }
    }
}
