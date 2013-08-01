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
public class Index {
  private volatile int hashCode = 0;

  private String projectID;
  private long uid;
  private short propertyID;
  private byte[] value;
  private String operation;
  private long timestamp;

  final int prime = 31;

  public Index(String projectID, long uid, short propertyID , byte[] value, String operation, long timestamp){
    this.projectID = projectID;
    this.uid = uid;
    this.propertyID = propertyID;
    this.value = value;
    this.operation = operation;
    this.timestamp = timestamp;
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
    int result = hashCode;
    if (result == 0) {
      result = 1;
      result = prime * result + projectID.hashCode();
      result = prime * result + (int)(timestamp ^ (timestamp >>> 32));
      result = prime * result + value.hashCode();
      result = prime * result + projectID.hashCode();
      result = prime * result + (int)(uid ^ (uid >>> 32));
      hashCode = result;
    }
    return result;
  }

  @Override
  public String toString(){
    return operation+"\t"+projectID+"\t"+propertyID+"\t"+timestamp+"\t"+value+"\t"+uid;
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
    return TimeUtil.getDayString(timestamp);
  }
}
