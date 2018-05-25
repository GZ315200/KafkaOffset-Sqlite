package com.unistack.tamboo.message.kafka.runtime;

/**
 * @author Gyges Zean
 * @date 2018/5/14
 */
public interface Herder {

    void start() throws Exception;

    void stop() throws Exception;

    RunnerStatus runnerStatus(String connName);

}
