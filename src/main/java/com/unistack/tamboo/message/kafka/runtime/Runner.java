package com.unistack.tamboo.message.kafka.runtime;

import com.unistack.tamboo.message.kafka.exceptions.ConfigException;
import com.unistack.tamboo.message.kafka.exceptions.RunnerNotFound;
import com.unistack.tamboo.message.kafka.runtime.entities.RunnerStateInfo;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 */
public class Runner {

    private static final Logger log = LoggerFactory.getLogger(Runner.class);

    private final Time time;
    private final String runner; // 启动的采集offset的名称

    private final String runnerHost;

    private final RunnerHerder runnerHerder;

    private RuntimeConfig runtimeConfig;

    public Runner(String runner,
                  String runnerHost) {
        this.runnerHost = runnerHost;
        this.time = SystemTime.SYSTEM;
        this.runner = runner;
        Objects.requireNonNull(runner, "runnerName should not be null");
        this.runnerHerder = new RunnerHerder(runner);
    }


    public boolean runnerConfig(Map<String, Object> config) {
        Objects.requireNonNull(config, "configuration map should not be null");
        runtimeConfig = new RuntimeConfig(config);
        try {
            runnerHerder.setConfig(runtimeConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * start runner
     */
    @SuppressWarnings(value = "unchecked")
    public boolean startRunner() {
        long start = time.milliseconds();
        log.info("Runner starting");
        try {
            runnerHerder.start();
            runnerHerder.onStartup(runner);
            log.info("Runner started,take {} mills", time.milliseconds() - start);
        } catch (Exception e) {
            runnerHerder.onFailure(runner, e);
            return false;
        }
        return true;
    }


    public boolean restartRunner(String runnerName) {
        Objects.requireNonNull(runnerName, "runnerName should not be null");
        String runtimeRunner = runtimeConfig.getString(RuntimeConfig.RUNNER_NAME);
        long start = time.milliseconds();
        try {
            log.info("Runner restarting");
            if (runtimeRunner.equals(runnerName)) {
                runnerHerder.start();
                log.info("Runner restarted,take {} mills", time.milliseconds() - start);
                runnerHerder.onStartup(runnerName);
                return true;
            } else {
                throw new ConfigException("no this runner configuration in cluster,please input them again.");
            }
        } catch (Exception e) {
            runnerHerder.onFailure(runtimeRunner, e);
            return false;
        }
    }


    /**
     * ensure this runner is running or not
     *
     * @param runnerName
     * @return
     */
    public boolean isRunning(String runnerName) {
        Objects.requireNonNull(runnerName, "runnerName should not be null");
        try {
            return runnerHerder.isRunning(runnerName);
        } catch (Exception e) {
            runnerHerder.onFailure(runnerName, e);
            return false;
        }
    }


    /**
     * whether runner is down or not.
     *
     * @param runnerName
     * @return
     */
    public boolean isDown(String runnerName) {
        Objects.requireNonNull(runnerName, "runnerName should not be null");
        try {
            if (!isRunning(runnerName)) {
                runnerHerder.onFailure(runnerName, null);
            }
            return true;
        } catch (Exception e) {
            runnerHerder.onFailure(runnerName, e);
            return false;
        }
    }


    /**
     * stop runner.
     */
    public boolean stopRunner(String runnerName) {
        Objects.requireNonNull(runnerName, "runnerName should not be null");
        String runtimeRunner = runtimeConfig.getString(RuntimeConfig.RUNNER_NAME);
        long start = time.milliseconds();
        log.info("Runner stopping");
        try {

            if (runtimeRunner.contains(runnerName)) {
                runnerHerder.stop();
                runnerHerder.onShutdown(runnerName);
                log.info("Runner stopped,take [} mills", time.milliseconds() - start);
            } else {
                throw new RunnerNotFound("this runner has been removed.");
            }
        } catch (Exception e) {
            runnerHerder.onFailure(runnerName, e);
            return false;
        }
        return true;
    }


    /**
     * package the runner state info.
     *
     * @return
     */
    public RunnerStateInfo runnerStateInfo(String runnerName) {
        Objects.requireNonNull(runnerName, "runnerName should not be null");
        String runtimeRunner = runtimeConfig.getString(RuntimeConfig.RUNNER_NAME);
        RunnerStateInfo runnerStateInfo = null;
        try {
            if (runtimeRunner.equals(runnerName))
                runnerStateInfo = new RunnerStateInfo(runnerHost, runnerHerder.runnerStatus(runnerName));
            else
                throw new RunnerNotFound("no this runner name in service.");
        } catch (Exception e) {
            runnerHerder.onFailure(runnerName, e);
        }
        return runnerStateInfo;
    }








}
