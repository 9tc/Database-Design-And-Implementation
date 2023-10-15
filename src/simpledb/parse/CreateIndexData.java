package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CreateIndexData {
    private String idxname;
    private String tblname;
    private String fldname;
}
