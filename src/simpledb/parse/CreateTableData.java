package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.record.Schema;

@AllArgsConstructor
@Getter
public class CreateTableData {
    private String tblname;
    private Schema schema;
}
