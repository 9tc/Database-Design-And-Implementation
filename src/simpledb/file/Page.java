package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page{
    private final ByteBuffer bb;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;
    public Page(int blockSize){
        bb = ByteBuffer.allocateDirect(blockSize);
    }

    public Page(byte[] bytes){
        bb = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset){
        return bb.getInt(offset);
    }

    public byte[] getBytes(int offset){
        bb.position(offset);
        int len = bb.getInt();
        byte[] b = new byte[len];
        bb.get(b);
        return b;
    }

    public String getString(int offset){
        return new String(getBytes(offset), CHARSET);
    }

    public void setInt(int offset, int n){
        bb.putInt(offset, n);
    }

    public void setBytes(int offset, byte[] bytes){
        bb.position(offset);
        bb.putInt(bytes.length);
        bb.put(bytes);
    }

    public void setString(int offset, String s){
        setBytes(offset, s.getBytes(CHARSET));
    }

    public static int maxLength(int len){
        return Integer.BYTES + (len * (int)CHARSET.newEncoder().maxBytesPerChar());
    }

    ByteBuffer contents(){
        bb.position(0);
        return bb;
    }
}
