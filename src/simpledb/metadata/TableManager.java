package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    public static final int MAX_NAME_LENGTH = 16;
    private Layout tcatLayout, fcatLayout;
    public TableManager(boolean isNew, Transaction transaction) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME_LENGTH);
        tcatSchema.addIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME_LENGTH);
        fcatSchema.addStringField("fldname", MAX_NAME_LENGTH);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);

        if(isNew){
            createTable("tblcat", tcatSchema, transaction);
            createTable("fldcat", fcatSchema, transaction);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction transaction) {
        Layout layout = new Layout(schema);
        TableScan tcat = new TableScan(transaction, "tblcat", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tableName);
        tcat.setInt("slotsize", layout.slotSize());
        tcat.close();

        TableScan fcat = new TableScan(transaction, "fldcat", fcatLayout);
        for(String fieldName: schema.fields()){
            fcat.insert();
            fcat.setString("tblname", tableName);
            fcat.setString("fldname", fieldName);
            fcat.setInt("type", schema.type(fieldName));
            fcat.setInt("length", schema.length(fieldName));
            fcat.setInt("offset", layout.offset(fieldName));
        }
        fcat.close();
    }

    public Layout getLayout(String tableName, Transaction transaction) {
        int size = -1;
        TableScan tcat = new TableScan(transaction, "tblcat", tcatLayout);
        while(tcat.next()){
            if(tcat.getString("tblname").equals(tableName)){
                size = tcat.getInt("slotsize");
                break;
            }
        }
        tcat.close();

        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<String, Integer>();
        TableScan fcat = new TableScan(transaction, "fldcat", fcatLayout);
        while(fcat.next()){
            if(fcat.getString("tblname").equals(tableName)){
                String fieldName = fcat.getString("fldname");
                int fieldType = fcat.getInt("type");
                int fieldLength = fcat.getInt("length");
                int offset = fcat.getInt("offset");

                offsets.put(fieldName, offset);
                schema.addField(fieldName, fieldType, fieldLength);
            }
        }
        fcat.close();
        return new Layout(schema, offsets, size);
    }
}
