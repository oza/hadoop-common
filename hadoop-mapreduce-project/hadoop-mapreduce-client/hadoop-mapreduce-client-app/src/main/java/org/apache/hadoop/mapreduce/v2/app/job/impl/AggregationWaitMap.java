package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;


public class AggregationWaitMap {
  private final ConcurrentHashMap<String, ArrayList<TaskAttemptCompletionEvent>> aggregationWaitMap;
  private final ConcurrentHashMap<String, String> taskToHostnameMap;
  private final ConcurrentHashMap<String, Boolean> taskToAggregated;
  private final ConcurrentMap<String,List<TaskAttemptCompletionEvent>> aggregatorMap;
  private final ReadWriteLock rwLock;
  private final Lock readLock;
  private final Lock writeLock;
  private static final Log LOG = LogFactory.getLog(AggregationWaitMap.class);
  
  public AggregationWaitMap() {
    this.rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.aggregationWaitMap = new ConcurrentHashMap<String, ArrayList<TaskAttemptCompletionEvent>>();
    this.taskToHostnameMap = new ConcurrentHashMap<String, String>();
    this.aggregatorMap = new ConcurrentHashMap<String, List<TaskAttemptCompletionEvent>>();
    this.taskToAggregated = new ConcurrentHashMap<String, Boolean>();
  }

  public ArrayList<TaskAttemptCompletionEvent> get(String hostname) {
    ArrayList <TaskAttemptCompletionEvent> ary;
    readLock.lock();
    try {
      ary  = aggregationWaitMap.get(hostname);
    } finally {
      readLock.unlock();
    }
    return ary;
  }
  
  
  public void registerHostname(String taskId, String hostname) {
    writeLock.lock();
    try {
      taskToHostnameMap.put(taskId, hostname);
    } finally {
      writeLock.unlock();
    }
    
  }
  
  public String getHostname(String taskId) {
    String hostname = null;
    readLock.lock();
    try {
      hostname = taskToHostnameMap.get(taskId);
    } finally {
      readLock.unlock();
    }
    return hostname;
  }
  
  public void put(String hostname, TaskAttemptCompletionEvent ev) {
    writeLock.lock();
    try {
      if (aggregationWaitMap.containsKey(hostname)) {
        ArrayList<TaskAttemptCompletionEvent> ary = aggregationWaitMap.get(hostname);
        ary.add(ev);
      } else {
        ArrayList<TaskAttemptCompletionEvent> ary = new ArrayList<TaskAttemptCompletionEvent>();
        ary.add(ev);
        aggregationWaitMap.put(hostname, ary);
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  public boolean containsKey(String hostname) {
    readLock.lock();
    try {
      return aggregationWaitMap.containsKey(hostname);
    } finally {
      readLock.unlock();
    }
  }
  
  public void put(String hostname, ArrayList<TaskAttemptCompletionEvent> events) {
    writeLock.lock();
    try {
      aggregationWaitMap.put(hostname, events);
    } finally {
      writeLock.unlock();
    }
  }
  
  
  public void clear() {
    writeLock.lock();
    try {
      aggregationWaitMap.clear();
    } finally {
      writeLock.unlock();
    }
  }
  
  public Set<Entry<String, ArrayList<TaskAttemptCompletionEvent>>> entrySet() {
    readLock.lock();
    Set<Entry<String, ArrayList<TaskAttemptCompletionEvent>>> set = 
        aggregationWaitMap.entrySet();
    readLock.unlock();
    
    return set;
  }
  
  public void clear(String hostname) {
    writeLock.lock();
    try {
      if (aggregationWaitMap.contains(hostname)) {
        ArrayList<TaskAttemptCompletionEvent> ary = aggregationWaitMap.get(hostname);
        ary.clear();
      } 
    } finally {
      writeLock.unlock();
    }
  }

  public ArrayList<TaskAttemptCompletionEvent> remove(String hostname) {
    ArrayList<TaskAttemptCompletionEvent> events = null;
    writeLock.lock();
    try {
      if (aggregationWaitMap.containsKey(hostname)) {
        events = aggregationWaitMap.remove(hostname);
      }
    } finally {
      writeLock.unlock();
    }
    return events;
  }

  public List<TaskAttemptID> getAggregationTargets(TaskAttemptID aggregator) {
    String taskId = aggregator.getTaskID().toString();
    List<TaskAttemptID> aggregationTargets = new ArrayList<TaskAttemptID>();
    List<TaskAttemptCompletionEvent> events;

    writeLock.lock();
    try {
      if (aggregatorMap.containsKey(taskId)) {
        events = aggregatorMap.get(taskId) ;

        if (events.size() > 1) {
          for(TaskAttemptCompletionEvent ev:events){
            TaskAttemptID attemptID = TypeConverter.fromYarn(ev.getAttemptId());
            aggregationTargets.add(attemptID);
          }
          taskToAggregated.put(taskId, new Boolean(true));
          LOG.info("[MR-4502] events.size: " + events.size());
        } else {
          // Dummy 
          aggregationTargets.add(aggregator);

          String hostname = getHostname(taskId);
          ArrayList<TaskAttemptCompletionEvent> ary = aggregationWaitMap.get(hostname);
          if (ary.isEmpty()) {
            ary = new ArrayList<TaskAttemptCompletionEvent>();
          }
          for (TaskAttemptCompletionEvent ev:events) {
            ary.add(ev);
          }
        }
      } else {
        // Dummy
        aggregationTargets.add(aggregator);
      }
    } finally {
      writeLock.unlock();
    }
    
    return aggregationTargets;
  }

  public boolean isAggregatable(String hostname,
      TaskAttemptId id, int aggregationThreshold) {
    boolean isAggregatable = false;
    // TODO Auto-generated method stub
    LOG.info("[MR-4502] hostname: " + hostname);
    LOG.info("[MR-4502] check aggregationWaitMap :" + aggregationWaitMap.containsKey(hostname));
    
    writeLock.lock();
    try {
      if (aggregationWaitMap.containsKey(hostname)) {
        ArrayList<TaskAttemptCompletionEvent> list = aggregationWaitMap.get(hostname);
        LOG.info("[MR-4502]" + " hostname is " + hostname + "list size is: " + list.size());
        if (list != null && (list.size() > aggregationThreshold)) {
          if (!aggregatorMap.containsKey(hostname)) {
            ArrayList<TaskAttemptCompletionEvent> events = aggregationWaitMap.remove(hostname);
            String taskId = id.getTaskId().toString();
            if (!taskToHostnameMap.containsKey(taskId)) {
              LOG.info("[MR-4502] Aggregator! taskId: " + taskId + ", hostname: " + hostname);
              taskToHostnameMap.put(taskId, hostname);
              aggregatorMap.put(taskId, events);
              isAggregatable = true;
              taskToAggregated.put(taskId, new Boolean(false));
            } else {
              LOG.info("[MR-4502] The task id is already used! taskId: " + taskId + ", hostname: " + hostname);
            }
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
    return isAggregatable;
  }

  public ArrayList<TaskAttemptCompletionEvent> removeAllEvents() {
    ArrayList<TaskAttemptCompletionEvent> events = new ArrayList<TaskAttemptCompletionEvent>();
    writeLock.lock();
    try {
      for (Entry<String, ArrayList<TaskAttemptCompletionEvent>> entry
          :aggregationWaitMap.entrySet()) {
        ArrayList<TaskAttemptCompletionEvent> ev = entry.getValue();
        events.addAll(ev);
      }
      aggregationWaitMap.clear();
    } finally {
      writeLock.unlock();
    }
    return events;
  }

  public List<TaskAttemptCompletionEvent> removeFinishedEvents(String taskId) {
    List<TaskAttemptCompletionEvent> events = null;
    writeLock.lock();
    try {
      if (!taskToAggregated.get(taskId)) {
        events = aggregatorMap.remove(taskId);
        String hostname = taskToHostnameMap.remove(taskId);
        if (aggregationWaitMap.containsKey(hostname)) {
          ArrayList<TaskAttemptCompletionEvent> evs = aggregationWaitMap.get(hostname);
          evs.addAll(events);
        } else {
          ArrayList<TaskAttemptCompletionEvent> evs = new ArrayList<TaskAttemptCompletionEvent>();
          evs.addAll(events);
          aggregationWaitMap.put(hostname, evs);
        }
        events = null;
      } else if (aggregatorMap.containsKey(taskId)) {
        // taskToAggregated is true, it is really aggregator task.
        events = aggregatorMap.remove(taskId);
        String hostname = taskToHostnameMap.remove(taskId);
        LOG.info("[MR-4502] taskId " + taskId + " and hostname " + hostname + " is unbinded");
      }
          
    } finally {
      writeLock.unlock();
    }
    return events;
  }

  public void abortAggregation(String taskId) {
    List<TaskAttemptCompletionEvent> events = null;
    
    writeLock.lock();
    try {
      if (taskToAggregated.get(taskId)) {
        if (aggregatorMap.containsKey(taskId)) {
          events = aggregatorMap.remove(taskId);
          String hostname = taskToHostnameMap.remove(taskId);
          taskToAggregated.put(taskId, new Boolean(false));
          
          if (!events.isEmpty() && aggregationWaitMap.containsKey(hostname)) {
            List<TaskAttemptCompletionEvent> evs = aggregationWaitMap.get(hostname);
            evs.addAll(events);
          } else {
            // TODO: throw IllegalStateException
          }
          LOG.info("[MR-4502] taskId " + taskId + " and hostname " + hostname + " is unbinded");
        } else{
            // TODO: throw IllegalStateException
        }
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  
  

}
