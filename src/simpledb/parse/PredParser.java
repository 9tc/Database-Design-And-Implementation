package simpledb.parse;

public class PredParser {
    private final Lexer lexer;

    public PredParser(String s){
        lexer = new Lexer(s);
    }

    public void field(){
        lexer.eatId();
    }

    public void constant(){
        if(lexer.matchStringConstant()){
            lexer.eatStringConstant();
        }else if(lexer.matchNumber()){
            lexer.eatNumber();
        }else{
            throw new BadSyntaxException();
        }
    }
}
