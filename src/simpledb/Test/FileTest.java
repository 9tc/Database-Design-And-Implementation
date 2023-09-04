package simpledb.Test;

import simpledb.file.BlockId;
import simpledb.file.FileManager;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

/**
 * @ClassName FileTest
 * @Description Fileシステムのテスト
 */
public class FileTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        FileManager fm = db.fileManager();

        BlockId blk = new BlockId("testfile", 2);
        Page p1 = new Page(fm.blockSize());

        int pos1 = 88;
        p1.setString(pos1, "abcd");

        int sz = Page.maxLength("abcd".length());

        int pos2 = pos1 + sz;
        p1.setInt(pos2, 345);
        fm.write(blk, p1);

        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);

        System.out.println("offset: " + pos2 + " contains: " + p2.getInt(pos2));
        System.out.println("offset: " + pos1 + " contains: " + p2.getInt(pos1));
    }
}
