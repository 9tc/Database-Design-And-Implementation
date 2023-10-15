package simpledb.parse;

import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

public class Lexer {
    private Collection<String> keywords;
    private final StreamTokenizer tokenizer;

    public Lexer(String s){
        initKeywords();
        tokenizer = new StreamTokenizer(new StringReader(s));
        tokenizer.ordinaryChar('.');
        tokenizer.wordChars('_', '_');
        tokenizer.lowerCaseMode(true);
        nextToken();
    }

    private void nextToken() {
        try {
            tokenizer.nextToken();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initKeywords() {
        keywords = Arrays.asList(
                "select", "from", "where", "and", "insert", "into", "values", "delete", "update", "set", "create", "table", "int", "varchar", "view", "as", "index", "on"
        );
    }

    public boolean matchDelim(char c) {
        return tokenizer.ttype == c;
    }

    public boolean matchNumber() {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER;
    }

    public boolean matchStringConstant() {
        return tokenizer.ttype == '\'';
    }

    public boolean matchKeyword(String s) {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval.equals(s);
    }

    public boolean matchId() {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval);
    }

    public void eatDelim(char c) {
        if(!matchDelim(c)){
            throw new BadSyntaxException();
        }
        nextToken();
    }

    public int eatNumber() {
        if(!matchNumber()){
            throw new BadSyntaxException();
        }
        int i = (int) tokenizer.nval;
        nextToken();
        return i;
    }

    public String eatStringConstant() {
        if(!matchStringConstant()){
            throw new BadSyntaxException();
        }
        String s = tokenizer.sval;
        nextToken();
        return s;
    }

    public String eatKeyword(String s) {
        if(!matchKeyword(s)){
            throw new BadSyntaxException();
        }
        String sval = tokenizer.sval;
        nextToken();
        return sval;
    }

    public String eatId() {
        if(!matchId()){
            throw new BadSyntaxException();
        }
        String sval = tokenizer.sval;
        nextToken();
        return sval;
    }
}
