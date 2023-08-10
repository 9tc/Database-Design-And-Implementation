package simpledb.record;

import java.util.HashMap;
import java.util.Map;

public class Layout {
    private Schema schema;
    private Map<String, Integer> offsets;
    private int slotSize;

    public Layout(Schema schema){
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES;
        for(String fieldName: schema.fields()){
            offsets.put(fieldName, pos);
            pos += lengthInBytes(fieldName);
        }
        slotSize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize){
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public Schema schema(){
        return schema;
    }

    public int offset(String fieldName){
        return offsets.get(fieldName);
    }

    public int slotSize(){
        return slotSize;
    }

    private int lengthInBytes(String fieldName){
        int fieldType = schema.type(fieldName);
        if(fieldType == java.sql.Types.INTEGER){
            return Integer.BYTES;
        }else{
            return schema.length(fieldName);
        }
    }
}
