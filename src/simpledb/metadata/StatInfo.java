package simpledb.metadata;

public class StatInfo {
    private int numBlocks;
    private int numRecords;

    public StatInfo(int numBlocks, int numRecords){
        this.numBlocks = numBlocks;
        this.numRecords = numRecords;
    }

    public int blocksAccessed(){
        return numBlocks;
    }

    public int recordsOutput(){
        return numRecords;
    }

    public int distinctValues(String fldname){
        return 1 + numRecords / 3;
    }
}
