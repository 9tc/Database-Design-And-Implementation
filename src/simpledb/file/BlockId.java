package simpledb.file;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class BlockId {
    private String filename;
    private int blockNum;

    public String toString() {
        return "[file " + filename + ", block " + blockNum + "]";
    }
}
