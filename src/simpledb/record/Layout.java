package simpledb.record;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public class Layout {
    @Getter
    private final Schema schema;
    private final Map<String, Integer> offsets;
    @Getter
    private final int slotSize;

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


    public int offset(String fieldName){
        return offsets.get(fieldName);
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
