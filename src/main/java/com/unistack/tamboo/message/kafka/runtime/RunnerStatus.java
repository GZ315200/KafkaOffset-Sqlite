package com.unistack.tamboo.message.kafka.runtime;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public class RunnerStatus extends AbstractStatus<String> {


    public RunnerStatus(String runnerId, State state, String msg) {
        super(runnerId, state, msg);
    }


    public interface Listener {
        /**
         * Invoked after connector has successfully been shutdown.
         *
         * @param runnerId The connector name
         */
        void onShutdown(String runnerId);

        /**
         * Invoked from the Connector using
         * Note that no shutdown event will follow after the task has been failed.
         *
         * @param runnerId The connector name
         * @param cause     Error raised from the connector.
         */
        void onFailure(String runnerId, Throwable cause);

        /**
         * Invoked after successful startup of the connector.
         *
         * @param runnerId The connector name
         */
        void onStartup(String runnerId);


    }
}
