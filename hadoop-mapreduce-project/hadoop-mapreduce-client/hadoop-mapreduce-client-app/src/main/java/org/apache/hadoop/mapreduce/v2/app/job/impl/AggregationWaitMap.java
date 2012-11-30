package org.apache.hadoop.mapreduce.v2.app.job.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;


public class AggregationWaitMap {
  private final ConcurrentHashMap<String, ArrayList<TaskAttemptCompletionEvent>> aggregationWaitMap;
  private final ReadWriteLock rwLock;
  private final Lock readLock;
  private final Lock writeLock;
  
  public AggregationWaitMap() {
    this.rwLock = new ReentrantReadWriteLock();
    this.readLock = rwLock.readLock();
    this.writeLock = rwLock.writeLock();
    this.aggregationWaitMap = new ConcurrentHashMap<String, ArrayList<TaskAttemptCompletionEvent>>();
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
  
  public void put(String hostname, TaskAttemptCompletionEvent ev) {
    writeLock.lock();
    try {
      if (aggregationWaitMap.contains(hostname)) {
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
  
  public boolean contains(String hostname) {
    readLock.lock();
    try {
      return aggregationWaitMap.contains(hostname);
    } finally {
      readLock.unlock();
    }
  }
  
  public void put(String hostname, ArrayList<TaskAttemptCompletionEvent> event) {
    writeLock.lock();
    try {
      aggregationWaitMap.put(hostname, event);
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
}
