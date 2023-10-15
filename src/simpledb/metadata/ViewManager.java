package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

public class ViewManager {
    private static final int MAX_VIEWDEF = 100;
    TableManager tm;

    public ViewManager(boolean isNew, TableManager tm, Transaction transaction){
        this.tm = tm;
        if(isNew){
            Schema schema = new Schema();
            schema.addStringField("viewname", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("viewdef", MAX_VIEWDEF);

            tm.createTable("viewcat", schema, transaction);
        }
    }

    public void createView(String viewName, String viewDef, Transaction transaction){
        Layout layout = tm.getLayout("viewcat", transaction);
        TableScan ts = new TableScan(transaction, "viewcat", layout);
        ts.insert();
        ts.setString("viewname", viewName);
        ts.setString("viewdef", viewDef);
        ts.close();
    }

    public String getViewDefinition(String viewName, Transaction transaction){
        String result = null;
        Layout layout = tm.getLayout("viewcat", transaction);
        TableScan ts = new TableScan(transaction, "viewcat", layout);
        while(ts.next()){
            if(ts.getString("viewname").equals(viewName)){
                result = ts.getString("viewdef");
                break;
            }
        }
        ts.close();
        return result;
    }
}
