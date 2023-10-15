package simpledb.plan;

import lombok.AllArgsConstructor;
import simpledb.metadata.MetadataManager;
import simpledb.parse.*;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.transaction.Transaction;

import java.util.Iterator;

@AllArgsConstructor
public class BasicUpdatePlanner implements UpdatePlanner{
    private MetadataManager metadataManager;

    @Override
    public int executeInsert(InsertData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.getTableName(), metadataManager);
        UpdateScan scan = (UpdateScan) plan.open();
        scan.insert();
        Iterator<Constant> iter = data.getValues().iterator();
        for(String fieldName : data.getFields()){
            scan.setValue(fieldName, iter.next());
        }
        scan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.getTableName(), metadataManager);
        plan = new SelectPlan(plan, data.getPredicate());
        UpdateScan scan = (UpdateScan) plan.open();
        int count = 0;

        while(scan.next()){
            scan.delete();
            count++;
        }
        scan.close();
        return count;
    }

    @Override
    public int executeUpdate(UpdateData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.getTableName(), metadataManager);
        plan = new SelectPlan(plan, data.getPredicate());
        UpdateScan scan = (UpdateScan) plan.open();
        int count = 0;
        while (scan.next()){
            Constant val = data.getNewExpression().evaluate(scan);
            scan.setValue(data.getFieldName(), val);
            count++;
        }
        scan.close();
        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction transaction) {
        metadataManager.createTable(data.getTableName(), data.getSchema(), transaction);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction transaction) {
        metadataManager.createView(data.getViewName(), data.viewDefinition(), transaction);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction transaction) {
        metadataManager.createIndex(data.getIndexName(), data.getTableName(), data.getFieldName(), transaction);
        return 0;
    }
}
