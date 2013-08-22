package com.xingcloud.xa.indextools;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 12-9-17
 * Time: 下午1:26
 * To change this template use File | Settings | File Templates.
 */
public class ThreadPool {
    private static Log LOGGER = LogFactory.getLog(ThreadPool.class);
    private ThreadPoolExecutor executor;
    private int DEFAULT_THREAD_NUM = 128;
    private long DEFAULT_TIMEOUT = 60*60*60;
    private String name;


    public ThreadPool(String name, int capacity) {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat(name);
        builder.setDaemon(true);
        ThreadFactory factory = builder.build();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(capacity, factory);
    }

    public ThreadPool() {
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("DEFAULT POOL");
        builder.setDaemon(true);
        ThreadFactory factory = builder.build();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(DEFAULT_THREAD_NUM, factory);
    }

    public ThreadPoolExecutor getPool() {
        return executor;
    }

    public FutureTask<?> addTask(Callable<?> task) {
        return (FutureTask<?>)executor.submit(task);
    }

    public void shutDown() {
        LOGGER.debug("------Shut down all tasks in thread pool------");
        executor.shutdown();
        /*Wait for all the tasks to finish*/
        try {
            boolean stillRunning = !executor.awaitTermination(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
            if (stillRunning) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    LOGGER.error("Thread pool's remain tasks' time out for " + DEFAULT_TIMEOUT + " seconds.", e);
                }
            }
        } catch (InterruptedException e) {
            try {
                Thread.currentThread().interrupt();
            } catch (Exception e1) {
                LOGGER.error("Thread pool has been interrupted!", e);
            }
        }
    }


}
