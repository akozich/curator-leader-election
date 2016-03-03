package org.curator.test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ElectedLeaderLifecycleRunner extends LeaderSelectorListenerAdapter implements Lifecycle {
    private final static Logger LOGGER = LoggerFactory.getLogger(ElectedLeaderLifecycleRunner.class);


    private final Lifecycle lifecycle;
    private final LeaderSelector leaderSelector;

    public ElectedLeaderLifecycleRunner(CuratorFramework client, String mutexPath, Lifecycle lifecycle) {
        this.lifecycle = lifecycle;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ZooKeeperCoordinatedTask-%d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LOGGER.error("Exception in zookeeper coordinated task thrown", e);
                    }
                }).build();
        ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);

        this.leaderSelector = new LeaderSelector(client, mutexPath, executorService, this);
        this.leaderSelector.autoRequeue();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        try {
            becomeLeader();
        } finally {
            relinquishLeadership();
        }
    }

    public void becomeLeader() {
        LOGGER.info("Starting {}", lifecycle);
        lifecycle.start();
    }

    public void relinquishLeadership() {
        LOGGER.info("Stopping {}", lifecycle);
        lifecycle.stop();
    }


    @Override
    public void start() {
        LOGGER.info("Starting leader selection");
        leaderSelector.start();
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping leader selection");
        leaderSelector.close();
    }
}
