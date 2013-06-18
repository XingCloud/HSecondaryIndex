package com.xingcloud.xa.secondaryindex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.xa.secondaryindex.model.Index;
import com.xingcloud.xa.secondaryindex.tail.Tail;
import com.xingcloud.xa.secondaryindex.utils.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/17/13
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexTailer extends Tail implements Runnable{

  private static final Log LOG = LogFactory.getLog(SecondaryIndexWriter.class);
  
  public IndexTailer(String configPath) {
    super(configPath);
    setBatchSize(6*10000);
    setLogProcessPerBatch(true);
  }

  @Override
  public void send(List<String> logs, long day) {
    try{
      Map<String, Map<String, List<Index>>> putsMap =  dispatchPuts(logs);
      ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(Constants.EXECUTOR_THREAD_COUNT,
        Constants.EXECUTOR_THREAD_COUNT,
        30,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<Runnable>());
      
      for(Map.Entry<String, Map<String, List<Index>>> entry: putsMap.entrySet()){
        threadPoolExecutor.execute(new HPutTask(entry.getKey(), entry.getValue()));
      }
      
      threadPoolExecutor.shutdown();
      boolean result = threadPoolExecutor.awaitTermination(30, TimeUnit.MINUTES);
      if (!result) {
        LOG.warn("put index timeout....throws this exception to tailer and quit this.");
        threadPoolExecutor.shutdownNow();
        throw new RuntimeException("put index timeout.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void run() {
    try{
      this.start();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
  
  private Map<String, Map<String, List<Index>>> dispatchPuts(List<String> logs) throws IOException {
    Map<String, Map<String, List<Index>>> putsMap = new HashMap<String, Map<String, List<Index>>>();
    ObjectMapper mapper = new ObjectMapper();
    for(String log: logs){
      Map<String,Object> data = mapper.readValue(log.getBytes(), Map.class);
      String tableName = "property_"+(String)data.get("pid")+"_index";
      long uid = Long.valueOf(String.valueOf(data.get("uid")));
      int propertyID = Integer.valueOf(String.valueOf(data.get("propertyID")));
      String oldValue = (String)data.get("old_value");
      String newValue = (String)data.get("new_value");
      String timestamp = (String)data.get("timestamp");
      Boolean needDelete = (Boolean)data.get("delete");
      
      Index put = new Index(tableName, uid, propertyID, newValue, "put", timestamp);   
      Index delete = null;
      
      if (needDelete){
        delete = new Index(tableName, uid, propertyID, oldValue, "delete", timestamp);    
      }

      //String hbaseAddress = UidMappingUtil.getInstance().ha sh(Long.valueOf(uid));
      String hbaseAddress = "HBASE";//todo wcl
      
      if(! putsMap.containsKey(tableName)){
        putsMap.put(tableName, new HashMap<String, List<Index>>());
      }
      
      if(! putsMap.get(tableName).containsKey(hbaseAddress)){
        putsMap.get(tableName).put(hbaseAddress, new ArrayList<Index>());   
      }
      
      if (delete != null ) putsMap.get(tableName).get(hbaseAddress).add(delete);
      putsMap.get(tableName).get(hbaseAddress).add(put);
      
    }  
    return putsMap;
  }
  
  
}
