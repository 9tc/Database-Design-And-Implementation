package simpledb.query;

public class Constant implements Comparable<Constant>{
    private Integer iValue = null;
    private String sValue = null;
    public Constant(Integer iValue){
        this.iValue = iValue;
    }
    public Constant(String sValue){
        this.sValue = sValue;
    }

    public Integer asInteger(){
        return iValue;
    }

    public String asString(){
        return sValue;
    }

    public boolean equals(Object obj){
        Constant c = (Constant) obj;
        if (iValue != null){
            return iValue.equals(c.iValue);
        }
        else{
            return sValue.equals(c.sValue);
        }
    }

    @Override
    public int compareTo(Constant constant) {
        return (iValue != null) ? iValue.compareTo(constant.iValue) : sValue.compareTo(constant.sValue);
    }

    public int hashCode(){
        return (iValue != null) ? iValue.hashCode() : sValue.hashCode();
    }

    public String toString(){
        return (iValue != null) ? iValue.toString() : sValue;
    }
}
