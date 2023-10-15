package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.query.Predicate;

import java.util.Collection;
import java.util.List;

@AllArgsConstructor
@Getter
public class QueryData {
    private List<String> fields;
    private Collection<String> tables;
    private Predicate predicate;

    public String toString(){
        StringBuilder result = new StringBuilder("select ");
        for (String fldname : fields)
            result.append(fldname).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        result.append(" from ");
        for (String tblname : tables)
            result.append(tblname).append(", ");
        result = new StringBuilder(result.substring(0, result.length() - 2));
        String predstring = predicate.toString();
        if (!predstring.isEmpty())
            result.append(" where ").append(predstring);
        return result.toString();
    }
}
