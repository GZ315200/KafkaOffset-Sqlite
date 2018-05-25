package com.unistack.tamboo.message.kafka.runtime;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 *
 * the connector task name, can not be repeated
 */
public class RunnerTaskId implements Serializable, Comparable<RunnerTaskId> {

    private final String runner; //connector name, it can be a username;

    private final int runnerId; //connector id, it can be a user id;


    public RunnerTaskId(@JsonProperty("runner") String runner, @JsonProperty("runnerId") int runnerId) {
        this.runner = runner;
        this.runnerId = runnerId;
    }

    @JsonProperty
    public String connector() {
        return runner;
    }

    @JsonProperty
    public int id() {
        return runnerId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RunnerTaskId that = (RunnerTaskId) o;

        if (runnerId != that.runnerId) return false;
        return runner.equals(that.runner);
    }

    @Override
    public int hashCode() {
        int result = runner.hashCode();
        result = 31 * result + runnerId;
        return result;
    }

    @Override
    public String toString() {
        return runner + "-" + runnerId;
    }


    @Override
    public int compareTo(RunnerTaskId o) {
        int connectorCmp = runner.compareTo(o.runner);
        if (connectorCmp != 0 )
            return connectorCmp;
        return Integer.compare(runnerId,o.runnerId);
    }
}
