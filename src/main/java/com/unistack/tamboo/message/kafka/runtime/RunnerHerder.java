package com.unistack.tamboo.message.kafka.runtime;

import com.unistack.tamboo.commons.utils.common.errors.NotFoundException;
import com.unistack.tamboo.message.kafka.storage.ConsumerGroupBackingStore;
import com.unistack.tamboo.message.kafka.storage.KafkaOffsetBackingStore;
import com.unistack.tamboo.message.kafka.storage.KafkaStatusBackingStore;
import com.unistack.tamboo.message.kafka.util.SystemTime;
import com.unistack.tamboo.message.kafka.util.Time;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 */
public class RunnerHerder implements Herder, RunnerStatus.Listener {

    protected final String runner;

    protected final KafkaStatusBackingStore statusBackingStore;

    private final KafkaOffsetBackingStore offsetBackingStore;

    private ConsumerGroupBackingStore consumerGroupBackingStore;

    public RunnerHerder(String runner) {
        this.runner = runner;
        Time time = SystemTime.SYSTEM;

        consumerGroupBackingStore.getConsumerGroups();
        this.statusBackingStore = new KafkaStatusBackingStore(time);
        this.offsetBackingStore = new KafkaOffsetBackingStore();
    }


    public boolean setConfig(RuntimeConfig config) throws Exception {
        statusBackingStore.configure(config);
        offsetBackingStore.configure(config);
        return true;
    }

    private void startServices() {
        this.offsetBackingStore.start();
        this.statusBackingStore.start();
    }

    private void stopServices() {
        this.offsetBackingStore.stop();
        this.statusBackingStore.stop();
    }



    public void start() throws Exception {
        startServices();
    }

    @Override
    public void stop() throws Exception {
        stopServices();
    }


    public boolean isRunning(String runnerName) {
        RunnerStatus status = statusBackingStore.get(runnerName);
        return status != null && status.state() == AbstractStatus.State.RUNNING;
    }


    @Override
    public RunnerStatus runnerStatus(String runnerName) {
        RunnerStatus runnerStatus = statusBackingStore.get(runnerName);
        if (runnerStatus == null)
            throw new NotFoundException("No status found for connector " + runnerName);
        return runnerStatus;
    }


    @Override
    public void onShutdown(String runner) {
        statusBackingStore.put(new RunnerStatus(runner, RunnerStatus.State.SHUTDOWN, runner));
    }

    @Override
    public void onFailure(String runner, Throwable cause) {
        statusBackingStore.put(new RunnerStatus(runner, RunnerStatus.State.FAILED, runner));
    }

    @Override
    public void onStartup(String runner) {
        statusBackingStore.put(new RunnerStatus(runner, RunnerStatus.State.RUNNING, runner));
    }
}
