package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.query.Expression;
import simpledb.query.Predicate;

@AllArgsConstructor
@Getter
public class UpdateData {
    private String tblname;
    private String fldname;
    private Expression newval;
    private Predicate predicate;
}
