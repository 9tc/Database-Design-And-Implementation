package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.query.Predicate;

@AllArgsConstructor
@Getter
public class DeleteData {
    private String tableName;
    private Predicate predicate;

    public String toString(){
        return "delete from " + tableName + " where " + predicate.toString();
    }
}
