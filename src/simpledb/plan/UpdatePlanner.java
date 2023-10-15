package simpledb.plan;

import simpledb.parse.CreateIndexData;
import simpledb.parse.CreateTableData;
import simpledb.parse.CreateViewData;
import simpledb.parse.DeleteData;
import simpledb.parse.InsertData;
import simpledb.parse.UpdateData;
import simpledb.transaction.Transaction;

public interface UpdatePlanner {
    public int executeInsert(InsertData data, Transaction transaction);
    public int executeDelete(DeleteData data, Transaction transaction);
    public int executeUpdate(UpdateData data, Transaction transaction);
    public int executeCreateTable(CreateTableData data, Transaction transaction);
    public int executeCreateView(CreateViewData data, Transaction transaction);
    public int executeCreateIndex(CreateIndexData data, Transaction transaction);
}
