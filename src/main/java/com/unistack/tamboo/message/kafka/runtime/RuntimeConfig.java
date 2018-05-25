package com.unistack.tamboo.message.kafka.runtime;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/27
 */
public class RuntimeConfig extends RunnerConfig {

    private static final ConfigDef CONFIG;

    public static final String GROUP_ID = "group.id";
    public static final String GROUP_ID_DOC = "consumer group id";

    public static final String INSERT_BATCH_SIZE = "batch.insert.size";
    public static final String INSERT_BATCH_SIZE_DOC = "batch insert size used when " +
            "offset insert into table was specified";

    public static final String CLASSNAME = "jdbc.classname";
    public static final String CLASSNAME_DOC = "jdbc classname used when " +
            "user create the jdbc driver access to database.";

    public static final String URL = "jdbc.url";
    public static final String URL_DOC = "jdbc url used when create jdbc connection.";

    public static final String USERNAME = "jdbc.username";
    public static final String USERNAME_DOC = "jdbc username used when access to database.";

    public static final String PASSWORD = "jdbc.password";
    public static final String PASSWORD_DOC = "jdbc password used when access to database.";


    public static final String RUNNER_NAME = "runner.name";
    public static final String RUNNER_NAME_DOC = "the runner name must be specified.";


    public static final String OFFSET_COMMIT_INTERVAL_MS_CONFIG = "offset.commit.interval.ms.config";

    static {
        CONFIG = baseConfigDef()
                .define(INSERT_BATCH_SIZE, ConfigDef.Type.INT, 1000, ConfigDef.Importance.HIGH, INSERT_BATCH_SIZE_DOC)
                .define(CLASSNAME, ConfigDef.Type.STRING, "org.sqlite.JDBC", ConfigDef.Importance.HIGH, CLASSNAME_DOC)
                .define(URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, URL_DOC)
                .define(USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, USERNAME_DOC)
                .define(PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, PASSWORD_DOC)
                .define(GROUP_ID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, GROUP_ID_DOC)
                .define(RUNNER_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, RUNNER_NAME_DOC)
                .define(OFFSET_COMMIT_INTERVAL_MS_CONFIG,ConfigDef.Type.LONG,10000,ConfigDef.Importance.HIGH,null)
        ;

    }

    public RuntimeConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }


    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }


}
