package simpledb.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class StatInfo {
    private final int accessedBlocks;
    private final int numRecords;

    public int distinctValues(String fldname){
        return 1 + numRecords / 3;
    }
}
