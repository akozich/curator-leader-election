package org.curator.test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class LeaderLatchLifecycleRunner implements LeaderLatchListener, Lifecycle {
    private final static Logger LOGGER = LoggerFactory.getLogger(ElectedLeaderLifecycleRunner.class);

    private final Lifecycle lifecycle;
    private final LeaderLatch leaderLatch;

    public LeaderLatchLifecycleRunner(CuratorFramework client, String mutexPath, Lifecycle lifecycle) {
        this.lifecycle = lifecycle;
        this.leaderLatch = new LeaderLatch(client, mutexPath, "", LeaderLatch.CloseMode.NOTIFY_LEADER);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ZooKeeperCoordinatedTask-%d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LOGGER.error("Exception in zookeeper coordinated task thrown", e);
                    }
                }).build();
        ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);

        this.leaderLatch.addListener(this, executorService);
    }

    @Override
    public void isLeader() {
        LOGGER.info("Starting {}", lifecycle);
        lifecycle.start();
    }

    @Override
    public void notLeader() {
        LOGGER.info("Stopping {}", lifecycle);
        lifecycle.stop();
    }

    @Override
    public void start() {
        LOGGER.info("Starting leader selection");
        try {
            leaderLatch.start();
        } catch (Exception e) {
            LOGGER.info("Failed to start LeaderLatch", e);
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping leader selection");
        try {
            leaderLatch.close();
        } catch (IOException e) {
            LOGGER.info("Failed to start LeaderLatch", e);
        }
    }
}
