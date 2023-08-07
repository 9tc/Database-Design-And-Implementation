package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.HashMap;

public class FileManager {

    private boolean isNew;
    private int blockSize;
    private File dbDirectory;
    private Map<String,RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File dbDirectory, int blockSize){
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        if(isNew){
            dbDirectory.mkdir();
        }
        for(String filename: dbDirectory.list()){
            if(filename.startsWith("temp")){
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(BlockId blk, Page page){
        try{
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.getChannel().read(page.contents());
        }catch (IOException e){
            throw new RuntimeException("cannot read block" + blk);
        }
    }

    private RandomAccessFile getFile(String filename) throws IOException{
        RandomAccessFile f = openFiles.get(filename);
        if(f == null){
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }

    public synchronized void write(BlockId blk, Page page){
        try{
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.getChannel().write(page.contents());
        }catch (IOException e) {
            throw new RuntimeException("cannot write block" + blk);
        }
    }

    public synchronized BlockId append(String filename){
        int newBlockNum = length(filename);
        BlockId blk = new BlockId(filename, newBlockNum);
        byte[] b = new byte[blockSize];
        try{
            RandomAccessFile f = getFile(blk.fileName());
            f.seek(blk.number() * blockSize);
            f.write(b);
        }catch (IOException e) {
            throw new RuntimeException("cannot append block" + blk);
        }
        return blk;
    }

    public boolean isNew(){
        return isNew;
    }

    public int length(String filename){
        try{
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blockSize);
        }catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public int blockSize(){
        return blockSize;
    }
}
