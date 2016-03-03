package org.curator.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class UnstableLifecycle implements Lifecycle {
    private final Logger LOGGER = LoggerFactory.getLogger(UnstableLifecycle.class);

    private final Object lifecycleMonitor = new Object();
    private volatile boolean running = false;
    private Random random;

    private final String name;

    public UnstableLifecycle(String name) {
        this.name = name;
        random = new Random(name.hashCode());
    }

    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (!running) {
                this.running = true;
                LOGGER.info("Running {}", name);

                int timeout = random.nextInt(1000) + 10000;
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted {}", name);
                }
                throw new RuntimeException();
            }
        }
    }

    @Override
    public void stop() {
        synchronized (lifecycleMonitor) {
            if (running) {
                this.running = false;
                LOGGER.info("Stopped {}", name);
            }
        }
    }

    @Override
    public String toString() {
        return "UnstableLifecycle{name='" + name + "\'}";
    }
}
