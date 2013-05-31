package com.xingcloud.xa.util;

/**
 * Created with IntelliJ IDEA.
 * User: witwolf
 * Date: 5/30/13
 * Time: 9:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class BytesUtil {

    public static byte[] CombineBytes(byte[]... bytesArrays) {
        int length = 0;
        for (byte[] bytes : bytesArrays) {
            length += bytes.length;
        }
        byte retBytes[] = new byte[length];
        int pos = 0;
        for (byte[] bytes : bytesArrays) {
            System.arraycopy(bytes, 0, retBytes, pos, bytes.length);
            pos += bytes.length;
        }
        return retBytes;
    }

    public static void  replaceBytes(byte[] src ,int srcPos, byte des[] , int desPos,int length){
        System.arraycopy(src,srcPos,des,desPos,length);
    }
}
