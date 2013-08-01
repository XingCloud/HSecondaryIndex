package com.xingcloud.xa.secondaryindex.model;

import com.xingcloud.xa.secondaryindex.utils.Constants;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

/**
 * User: liuxiong
 * Date: 13-7-31
 * Time: 下午8:27
 */
public class IndexIgnoreOperationComparator implements Comparator<Index> {

    @Override
    public int compare(Index ixLeft, Index ixRight) {
        String left = ixLeft.toStringIgnoreOperation();
        String right = ixRight.toStringIgnoreOperation();
        return left.compareTo(right);
    }

    public static void main(String[] args){
        long timestamp = System.currentTimeMillis();
        Index ix1 = new Index("sof-dsk", 123456L, (short)20, Bytes.toBytes(20130713123456L),
                Constants.OPERATION_PUT, timestamp);

        Index ix2 = new Index("sof-dsk", 123456L, (short)20, Bytes.toBytes(20130713123457L),
                Constants.OPERATION_DELETE, timestamp);

        IndexIgnoreOperationComparator ic = new IndexIgnoreOperationComparator();

        System.out.println(ix1.compareTo(ix2));
        System.out.println(ic.compare(ix1, ix2));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            ix1.compareTo(ix2);
        }
        System.out.println(System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            ic.compare(ix1, ix2);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

}
