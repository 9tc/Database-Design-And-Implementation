package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CreateIndexData {
    private String indexName;
    private String tableName;
    private String fieldName;
}
