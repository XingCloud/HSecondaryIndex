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
        Index ix1 = new Index("sof-dsk", 123456L, (short)20, Bytes.toBytes(20130713123456L),
                Constants.OPERATION_PUT, System.currentTimeMillis());

        Index ix2 = new Index("sof-dsk", 123456L, (short)20, Bytes.toBytes(20130713123456L),
                Constants.OPERATION_DELETE, System.currentTimeMillis());

        IndexIgnoreOperationComparator ic = new IndexIgnoreOperationComparator();
        long start = System.currentTimeMillis();
        System.out.println(ic.compare(ix1, ix2));
        for (int i = 0; i < 100000; i++) {
            ic.compare(ix1, ix2);
        }
        System.out.println(System.currentTimeMillis() - start);

        int[] ints = {1,2,3,4,4,4,5,5,6,7};
        for (int i = 0; i < ints.length; ){
            int startInt = ints[i];
            int j;
            for (j = i + 1; j < ints.length; j++){
                int endInt = ints[j];
                if (startInt != endInt){
                    break;
                }
            }

            System.out.println("i= " + i + ", j=" + j);
            i = j;
        }

    }

}
