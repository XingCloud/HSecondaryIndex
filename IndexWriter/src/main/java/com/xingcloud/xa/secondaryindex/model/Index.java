package com.xingcloud.xa.secondaryindex.model;

import com.xingcloud.xa.secondaryindex.utils.TimeUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/20/13
 * Time: 7:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class Index implements Comparable<Index> {
  final int prime = 31;

  private String projectID;
  private long uid;
  private short propertyID;
  private byte[] value;
  private String operation;
  private long timestamp;
  private String date;

  public Index(String projectID, long uid, short propertyID , byte[] value, String operation, long timestamp){
    this.projectID = projectID;
    this.uid = uid;
    this.propertyID = propertyID;
    this.value = value;
    this.operation = operation;
    this.timestamp = timestamp;
    this.date = TimeUtil.getDayString(timestamp);
  }

  public String getProjectID() {
    return projectID;
  }

  public long getUid() {
    return uid;
  }

  public short getPropertyID() {
    return propertyID;
  }

  public byte[] getValue() {
    return value;
  }

  public String getOperation() {
    return operation;
  }

  @Override
  public boolean equals(Object o){
    return (this.hashCode() == o.hashCode());
  }

  @Override
  public int hashCode(){
    int result = 1;
    result = prime * result + projectID.hashCode();
    result = prime * result + date.hashCode();
    result = prime * result + value.hashCode();
    result = prime * result + projectID.hashCode();
    result = prime * result + (int)(uid ^ (uid >>> 32));
    return result;
  }

  @Override
  public String toString(){
    return operation+"\t"+projectID+"\t"+propertyID+"\t"+timestamp+"\t"+value+"\t"+uid;
  }

    @Override
    public int compareTo(Index ix){
        int ret = projectID.compareTo(ix.projectID);
        if(ret != 0)
            return ret;

        ret = propertyID < ix.propertyID ? -1: (propertyID == ix.propertyID ? 0: 1);
        if(ret != 0)
            return ret;

        ret = timestamp < ix.timestamp ? -1: (timestamp == ix.timestamp ? 0: 1);
        if(ret != 0)
            return ret;

        ret = uid < ix.uid ? -1: (uid == ix.uid ? 0: 1);
        if(ret != 0)
            return ret;

        return compareByteArray(value, ix.value);
    }

    private int compareByteArray(byte[] left, byte[] right){
        if(left == null){
            if(right == null)
                return 0;
            else
                return 1;
        } else {
            if(right == null)
                return -1;

            // left and right are both not null
            int minLength = left.length < right.length ? left.length: right.length;
            for (int i = 0; i < minLength; i++) {
                int ret = left[i] < right[i] ? -1: (left[i] == right[i] ? 0: 1);
                if(ret != 0)
                    return ret;
            }

            // if we reach here, left[0: minLength] == right[0: minLength]

            if(left.length > minLength)
                return -1;
            if(right.length > minLength)
                return 1;

            return 0;
        }
    }

  public String toStringIgnoreOperation(){
      return projectID + propertyID + timestamp + Arrays.toString(value) + uid;
  }

  public void setProjectID(String projectID) {
    this.projectID = projectID;
  }

  public void setUid(long uid) {
    this.uid = uid;
  }

  public void setPropertyID(short propertyID) {
    this.propertyID = propertyID;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }


  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getDate() {
    return date;
  }
}
