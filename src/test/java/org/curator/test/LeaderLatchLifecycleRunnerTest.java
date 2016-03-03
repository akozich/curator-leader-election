package org.curator.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class LeaderLatchLifecycleRunnerTest {
    private TestingServer zkTestServer;
    private CuratorFramework cli;
    private LeaderLatchLifecycleRunner runnerA;
    private LeaderLatchLifecycleRunner runnerB;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServer(2181);
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(1000));
        cli.start();

        runnerA = new LeaderLatchLifecycleRunner(cli, "/somePath", new UnstableLifecycle("A"));
        runnerB = new LeaderLatchLifecycleRunner(cli, "/somePath", new UnstableLifecycle("B"));
    }

    @Test
    public void shouldRun() throws Exception {
        runnerA.start();
        runnerB.start();

        Object monitor = new Object();
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            zkTestServer.restart();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, 0, 3000);


        timer.scheduleAtFixedRate(
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            int available = System.in.available();
                            if (available > 0) {
                                synchronized (monitor) {
                                    monitor.notifyAll();
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, 0, 1000);

        synchronized (monitor) {
            monitor.wait();
        }

    }

    @After
    public void shutDown() throws Exception {
        runnerA.stop();
        runnerB.stop();
        cli.close();
        zkTestServer.stop();
    }
}