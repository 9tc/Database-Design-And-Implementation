package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.query.Expression;
import simpledb.query.Predicate;

@AllArgsConstructor
@Getter
public class UpdateData {
    private String tableName;
    private String fieldName;
    private Expression newExpression;
    private Predicate predicate;
}
