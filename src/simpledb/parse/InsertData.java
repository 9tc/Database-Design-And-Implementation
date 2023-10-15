package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import simpledb.query.Constant;

import java.util.List;

@AllArgsConstructor
@Getter
public class InsertData {
    private String tblname;
    private List<String> fields;
    private List<Constant> values;
}
