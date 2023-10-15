package simpledb.parse;

import simpledb.query.Constant;
import simpledb.query.Expression;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Parser {
    private final Lexer lexer;

    public Parser(String s){
        lexer = new Lexer(s);
    }

    public String field(){
        return lexer.eatId();
    }

    public Constant constant() {
        if (lexer.matchStringConstant()) {
            return new Constant(lexer.eatStringConstant());
        } else {
            return new Constant(lexer.eatNumber());
        }
    }

    public Expression expression() {
        if (lexer.matchId()){
            return new Expression(field());
        }else{
            return new Expression(constant());
        }
    }

    public Term term() {
        Expression lhs = expression();
        lexer.eatDelim('=');
        Expression rhs = expression();
        return new Term(lhs, rhs);
    }

    public Predicate predicate() {
        Predicate predicate = new Predicate(term());
        if(lexer.matchKeyword("and")){
            lexer.eatKeyword("and");
            predicate.conjoinWith(predicate());
        }
        return predicate;
    }

    public QueryData query() {
        lexer.eatKeyword("select");
        List<String> fields = selectList();
        lexer.eatKeyword("from");
        Collection<String> tables = tableList();
        Predicate predicate = new Predicate();
        if(lexer.matchKeyword("where")){
            lexer.eatKeyword("where");
            predicate = predicate();
        }
        return new QueryData(fields, tables, predicate);
    }

    private List<String> selectList() {
        List<String> fields = new ArrayList<>();
        fields.add(field());
        while(lexer.matchDelim(',')){
            lexer.eatDelim(',');
            fields.add(field());
        }
        return fields;
    }

    private Collection<String> tableList() {
        Collection<String> tables = new ArrayList<>();
        tables.add(lexer.eatId());
        while(lexer.matchDelim(',')){
            lexer.eatDelim(',');
            tables.add(lexer.eatId());
        }
        return tables;
    }

    public Object updateCommand(){
        if(lexer.matchKeyword("insert")) return insert();
        if(lexer.matchKeyword("delete")) return delete();
        if(lexer.matchKeyword("update")) return update();
        return create();
    }

    private Object create() {
        lexer.eatKeyword("create");
        if(lexer.matchKeyword("table")) return createTable();
        if(lexer.matchKeyword("view")) return createView();
        return createIndex();
    }

    public DeleteData delete(){
        lexer.eatKeyword("delete");
        lexer.eatKeyword("from");
        String tblname = lexer.eatId();
        Predicate predicate = new Predicate();
        if(lexer.matchKeyword("where")){
            lexer.eatKeyword("where");
            predicate = predicate();
        }
        return new DeleteData(tblname, predicate);
    }

    public InsertData insert(){
        lexer.eatKeyword("insert");
        lexer.eatKeyword("into");
        String tblname = lexer.eatId();
        lexer.eatDelim('(');
        List<String> fields = fieldList();
        lexer.eatDelim(')');
        lexer.eatKeyword("values");
        lexer.eatDelim('(');
        List<Constant> values = constList();
        lexer.eatDelim(')');
        return new InsertData(tblname, fields, values);
    }

    private List<String> fieldList() {
        List<String> fields = new ArrayList<>();
        fields.add(field());
        if(lexer.matchDelim(',')){
            lexer.eatDelim(',');
            fields.addAll(fieldList());
        }
        return fields;
    }

    private List<Constant> constList(){
        List<Constant> consts = new ArrayList<>();
        consts.add(constant());
        if(lexer.matchDelim(',')){
            lexer.eatDelim(',');
            consts.addAll(constList());
        }
        return consts;
    }

    public UpdateData update(){
        lexer.eatKeyword("update");
        String tblname = lexer.eatId();
        lexer.eatKeyword("set");
        String fieldName = field();
        lexer.eatDelim('=');
        Expression newExpr = expression();
        Predicate predicate = new Predicate();
        if(lexer.matchKeyword("where")){
            lexer.eatKeyword("where");
            predicate = predicate();
        }
        return new UpdateData(tblname, fieldName, newExpr, predicate);
    }

    public CreateTableData createTable(){
        lexer.eatKeyword("table");
        String tblname = lexer.eatId();
        lexer.eatDelim('(');
        Schema fields = fieldDefinitions();
        lexer.eatDelim(')');
        return new CreateTableData(tblname, fields);
    }

    private Schema fieldDefinitions(){
        Schema schema = fieldDefinition();
        if(lexer.matchDelim(',')){
            lexer.eatDelim(',');
            schema.addAll(fieldDefinitions());
        }
        return schema;
    }

    private Schema fieldDefinition(){
        return fieldType(field());
    }

    private Schema fieldType(String fldname) {
        Schema schema = new Schema();
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int");
            schema.addIntField(fldname);
        } else {
            lexer.eatKeyword("varchar");
            lexer.eatDelim('(');
            int strLen = lexer.eatNumber();
            lexer.eatDelim(')');
            schema.addStringField(fldname, strLen);
        }
        return schema;
    }

    public CreateViewData createView(){
        lexer.eatKeyword("view");
        String viewname = lexer.eatId();
        lexer.eatKeyword("as");
        QueryData qd = query();
        return new CreateViewData(viewname, qd);
    }

    public CreateIndexData createIndex(){
        lexer.eatKeyword("index");
        String idxname = lexer.eatId();
        lexer.eatKeyword("on");
        String tblname = lexer.eatId();
        lexer.eatDelim('(');
        String fldname = field();
        lexer.eatDelim(')');
        return new CreateIndexData(idxname, tblname, fldname);
    }
}

