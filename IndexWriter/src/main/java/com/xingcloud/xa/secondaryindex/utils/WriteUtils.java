package com.xingcloud.xa.secondaryindex.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:31
 * To change this template use File | Settings | File Templates.
 */
public class WriteUtils {
    private static Log LOG = LogFactory.getLog(WriteUtils.class);

    public static byte[] getFiveBytes(long suid) {
        byte[] rk = new byte[5];
        rk[0] = (byte) (suid>>32 & 0xff);
        rk[1] = (byte) (suid>>24 & 0xff);
        rk[2] = (byte) (suid>>16 & 0xff);
        rk[3] = (byte) (suid>>8 & 0xff);
        rk[4] = (byte) (suid & 0xff);
        return rk;
    }

    public static byte[] getUIIndexRowKey(short propertyID, String date, String attrVal) {
        return bytesCombine(Bytes.toBytes(propertyID), Bytes.toBytes(Integer.valueOf(date)),
                Bytes.toBytesBinary(attrVal));
    }

    public static String getUIIndexTableName(String pID) {
        return Constants.PROPERTY_TABLE_PREFIX + pID + Constants.INDEX_TABLE_SUFFIX;
    }
  
    public static byte[] bytesCombine(byte[]... bytesArrays){
        int length = 0;
        for (byte[] bytes: bytesArrays){
            length += bytes.length;
        }
        byte[] combinedBytes = new byte[length];
        int index = 0;
        for (byte[] bytes: bytesArrays){
            for(byte b: bytes){
                combinedBytes[index] = b;
                index++;
            }
        }
        return combinedBytes;
    }


    public static int getInnerUid(byte[] uid) {
        uid[0] = (byte) (uid[0] & 0);
        return Bytes.toInt(uid);
    }



    public static int getAttrValFromIndexRK(byte[] rk) {
        byte[] val = Arrays.copyOfRange(rk, 6, rk.length);
        return Bytes.toInt(val);
    }

    public static String getAttrFromVal(byte[] val, boolean isLong) {
        if (isLong) {
            return String.valueOf(Bytes.toLong(val));
        } else {
            return Bytes.toString(val);
        }
    }

    public static Pair<Long, Long> getLocalSEUidOfBucket(int bucketNum, int offsetBucket) {
        long startBucket = offsetBucket;
        startBucket = startBucket << 32;
        long endBucket = 0;
        if (offsetBucket + bucketNum >= 256) {
            endBucket = (1l << 40) - 1l;
        } else {
            endBucket = offsetBucket + bucketNum;
            endBucket = endBucket << 32;
        }

        return new Pair<Long, Long>(startBucket, endBucket);
    }

    public static long getSamplingUid(byte[] qualifier) {
      byte[] suid = new byte[8];
      suid[0] = 0;
      suid[1] = 0;
      suid[2] = 0;
      for (int i=3; i<8; i++) {
        suid[i] = qualifier[i-3];
      }
      return Bytes.toLong(suid);
    }

}
