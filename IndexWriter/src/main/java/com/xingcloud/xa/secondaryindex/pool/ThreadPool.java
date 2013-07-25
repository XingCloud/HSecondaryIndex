package com.xingcloud.xa.secondaryindex.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-7-11
 * Time: 下午6:41
 * To change this template use File | Settings | File Templates.
 */
public class ThreadPool {
    private static Log logger = LogFactory.getLog(ThreadPool.class);
    private ThreadPoolExecutor executor;
    private int DEFAULT_THREAD_NUM = 10;
    private long TIMEOUT = Integer.MAX_VALUE;
    private boolean isShutDown = false;

    private static ThreadPool m_instance;

    private ThreadPool() {
        logger.info("First time init batch put task pool");
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("Batch put task pool");
        builder.setDaemon(true);
        ThreadFactory factory = builder.build();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(DEFAULT_THREAD_NUM, factory);
    }

    public synchronized static ThreadPool getInstance() {
        if (m_instance == null) {
            m_instance = new ThreadPool();
        }
        return m_instance;
    }

    public ThreadPoolExecutor getPool() {
        return executor;
    }

    public FutureTask<?> addInnerTask(Callable<?> task) {
        return (FutureTask<?>)executor.submit(task);
    }

    public synchronized void shutDownNow() {
        if (!isShutDown) {
            logger.info("------Shut down all tasks in batch put task thread pool------");
            executor.shutdown();
            /*Wait for all the tasks to finish*/
            try {
                boolean stillRunning = !executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
                if (stillRunning) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        logger.error("Thread pool remain batch put tasks' time out of time for " + TIMEOUT + " seconds.", e);
                    }
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    logger.error("Batch put thread pool has been interrupted!", e);
                }
            }
            isShutDown = true;
        }
    }

}
