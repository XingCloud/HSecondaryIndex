package com.xingcloud.xa.secondaryindex.model;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/20/13
 * Time: 7:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class Index {
  private String projectID;
  private long uid;
  private int propertyID;//1 2 3
  private String value="";
  private String operation;
  private long timestamp; //System.currentTimeMillis()

  private static final TimeZone TZ = TimeZone.getTimeZone("GMT+8");
  private static SimpleDateFormat sdf;
  static {
      sdf = new SimpleDateFormat("yyyyMMdd");
      sdf.setTimeZone(TZ);
  }



  public Index(String projectID, long uid, int propertyID , String value, String operation, long timestamp){
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

  public int getPropertyID() {
    return propertyID;
  }

  public String getValue() {
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
    return (projectID + "_" + propertyID + "_" + getDate()+"_"+value +"_"+ uid).hashCode();
  }


  @Override
  public String toString(){
    return operation+"\t"+projectID+"\t"+propertyID+"\t"+timestamp+"\t"+value+"\t"+uid;
  }
  
  public void setProjectID(String projectID) {
    this.projectID = projectID;
  }

  public void setUid(long uid) {
    this.uid = uid;
  }

  public void setPropertyID(int propertyID) {
    this.propertyID = propertyID;
  }

  public void setValue(String value) {
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
    return sdf.format(timestamp);
  }
}
