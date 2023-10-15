package simpledb.parse;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CreateViewData {
    private String viewName;
    private QueryData queryData;

    public String viewDefinition(){
        return queryData.toString();
    }
}
