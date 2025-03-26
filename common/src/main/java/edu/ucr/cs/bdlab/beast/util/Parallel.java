/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.ucr.cs.bdlab.beast.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Vector;
import java.util.function.BiFunction;

/**
 * Some primitives to provide parallel processing over arrays and lists
 * @author Ahmed Eldawy
 *
 */
public class Parallel {
  
  static final Log LOG = LogFactory.getLog(Parallel.class);

  private Parallel() { /* Enforce static use only */ }
  
  /**
   * An interface that is implemented by users to loop over a partial array.
   * @author Ahmed Eldawy
   *
   */
  public static class RunnableRangeThread<T> extends Thread {
    private int i1;
    private int i2;
    private BiFunction<Integer, Integer,T> runnableRange;
    private T result;
    
    protected RunnableRangeThread(BiFunction<Integer, Integer,T> runnableRange, int i1, int i2) {
      super("Worker ["+i1+","+i2+")");
      this.i1 = i1;
      this.i2 = i2;
      this.runnableRange = runnableRange;
    }
    
    @Override
    public void run() {
      this.result = this.runnableRange.apply(i1, i2);
    }
    
    public T getResult() {
      return result;
    }
  }
  
  public static <T> List<T> forEach(int size, BiFunction<Integer, Integer,T> r) throws InterruptedException {
    return forEach(0, size, r, Runtime.getRuntime().availableProcessors());
  }

  public static <T> List<T> forEach(int size, BiFunction<Integer, Integer,T> r, int parallelism) throws InterruptedException {
    return forEach(0, size, r, parallelism);
  }

  public static <T> List<T> forEach(int start, int end, BiFunction<Integer, Integer,T> r, int parallelism) throws InterruptedException {
    Vector<T> results = new Vector<T>();
    if (end <= start)
      return results;
    final Vector<Throwable> exceptions = new Vector<Throwable>();
    Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        exceptions.add(ex);
      }
    };
    
    // Put an upper bound on parallelism to avoid empty ranges
    if (parallelism > (end - start))
      parallelism = end - start;
    if (parallelism == 1) {
      // Avoid creating threads
      results.add(r.apply(start, end));
    } else {
      LOG.info("Creating "+parallelism+" threads");
      final int[] partitions = new int[parallelism + 1];
      for (int i_thread = 0; i_thread <= parallelism; i_thread++)
        partitions[i_thread] = i_thread * (end - start) / parallelism + start;
      final Vector<RunnableRangeThread<T>> threads = new Vector<RunnableRangeThread<T>>();
      for (int i_thread = 0; i_thread < parallelism; i_thread++) {
        RunnableRangeThread<T> thread = new RunnableRangeThread<T>(r,
            partitions[i_thread], partitions[i_thread+1]);
        thread.setUncaughtExceptionHandler(h);
        threads.add(thread);
        threads.lastElement().start();
      }
      for (int i_thread = 0; i_thread < parallelism; i_thread++) {
        threads.get(i_thread).join();
        results.add(threads.get(i_thread).getResult());
      }
      if (!exceptions.isEmpty())
        throw new RuntimeException(exceptions.size()+" unhandled exceptions",
            exceptions.firstElement());
    }
    return results;
  }

}
