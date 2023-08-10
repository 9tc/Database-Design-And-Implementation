package simpledb.record;

public class RID {
    private int blockNum;
    private int slot;

    public RID(int blockNum, int slot){
        this.blockNum = blockNum;
        this.slot = slot;
    }

    public int blockNumber(){
        return blockNum;
    }

    public int slot(){
        return slot;
    }

    public boolean equals(Object obj){
        RID r = (RID) obj;
        return blockNum == r.blockNum && slot == r.slot;
    }

    public int hashCode(){
        return toString().hashCode();
    }

    public String toString(){
        return "[" + blockNum + ", " + slot + "]";
    }
}
