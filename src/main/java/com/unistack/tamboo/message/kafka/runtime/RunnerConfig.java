package com.unistack.tamboo.message.kafka.runtime;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Type;
import static org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 * 用于kafka
 */
public class RunnerConfig extends AbstractConfig {


    public static final String USER_STORAGE_TOPIC = "user.storage.topic";
    public static final String USER_STORAGE_TOPIC_DOC = "this topic only use by user," +
            "someone use this topic to store their pretreatment data";

    public static final String USER_ANALYSIS_TOPIC = "user.analysis.topic";
    public static final String USER_ANALYSIS_TOPIC_DOC = "this topic only use by a user," +
            "someone use this topic to store their analysis data,these data have been analysis";


    public static final String KAFKA_AC_USERNAME = "kafka.acl.username";
    public static final String KAFKA_AC_USERNAME_DEFAULT = "guest";
    public static final String KAFKA_AC_USERNAME_DOC = "<p>this configuration item " +
            "for kafka acl username,this must be set</p>";


    public static final String KAFKA_AC_PASSWORD = "kafka.acl.password";
    public static final String KAFKA_AC_PASSWORD_DEFAULT = "guest";
    public static final String KAFKA_AC_PASSWORD_DOC = "this configuration item " +
            "for kafka acl password,this must be set";

    public static final String OFFSET_STORAGE_TABLE_PATH = "offset.storage.table.path";
    public static final String OFFSET_STORAGE_TABLE_PATH_DEFAULT = "/opt/apps/data/";


    public static final String OFFSET_STORAGE_TABLE = "offset.storage.table";
    public static final String OFFSET_STORAGE_TABLE_DEFAULT = "test";
    public static final String OFFSET_STORAGE_TABLE_DOC = "";

    public static final String STATUS_STORAGE_TOPIC = "status.storage.topic";
    public static final String STATUS_STORAGE_TOPIC_DEFAULT = "runner_status";
    public static final String STATUS_STORAGE_TOPIC_DOC = "storage for runner status";


    public static final String STATUS_STORAGE_PARTITIONS_CONFIG = "status.storage.partitions.config";
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG_DEFAULT = "1";
    public static final String STATUS_STORAGE_PARTITIONS_CONFIG_DOC = "status storage topic partition";


    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG = "status.storage.replication.factor.config";
    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DEFAULT = "1";
    public static final String STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC = "status storage topic replication";


    public static final String USER_TOPIC_PARTITIONS_CONFIG
            = "user.topic.partitions.config";
    public static final String USER_TOPIC_PARTITIONS_CONFIG_DEFAULT = "1";
    public static final String USER_TOPIC_PARTITIONS_CONFIG_DOC = "";


    public static final String USER_TOPIC_REPLICA_CONFIG = "user.topic.replica.config";
    public static final String USER_TOPIC_REPLICA_CONFIG_DEFAULT = "1";
    public static final String USER_TOPIC_REPLICA_CONFIG_DOC = "";


    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(KAFKA_AC_USERNAME, Type.STRING, KAFKA_AC_USERNAME_DEFAULT, Importance.HIGH, KAFKA_AC_USERNAME_DOC)
                .define(KAFKA_AC_PASSWORD, Type.STRING, KAFKA_AC_PASSWORD_DEFAULT, Importance.HIGH, KAFKA_AC_PASSWORD_DOC)
                .define(OFFSET_STORAGE_TABLE, Type.STRING, OFFSET_STORAGE_TABLE_DEFAULT, Importance.HIGH, OFFSET_STORAGE_TABLE_DOC)
                .define(STATUS_STORAGE_TOPIC, Type.STRING, STATUS_STORAGE_TOPIC_DEFAULT, Importance.HIGH, STATUS_STORAGE_TOPIC_DOC)
                .define(USER_TOPIC_PARTITIONS_CONFIG, Type.INT, USER_TOPIC_PARTITIONS_CONFIG_DEFAULT, Importance.HIGH, USER_TOPIC_PARTITIONS_CONFIG_DOC)
                .define(USER_TOPIC_REPLICA_CONFIG, Type.INT, USER_TOPIC_REPLICA_CONFIG_DEFAULT, Importance.HIGH, USER_TOPIC_REPLICA_CONFIG_DOC)
                .define(USER_STORAGE_TOPIC, Type.STRING, null, Importance.HIGH, USER_STORAGE_TOPIC_DOC)
                .define(USER_ANALYSIS_TOPIC, Type.STRING, null, Importance.HIGH, USER_ANALYSIS_TOPIC_DOC)
                .define(STATUS_STORAGE_PARTITIONS_CONFIG, Type.STRING, STATUS_STORAGE_PARTITIONS_CONFIG_DEFAULT, Importance.MEDIUM, STATUS_STORAGE_PARTITIONS_CONFIG_DOC)
                .define(STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, Type.STRING, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DEFAULT, Importance.MEDIUM, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG_DOC)
                ;
    }

    public RunnerConfig(ConfigDef definition, Map<String, Object> props) {
        super(definition, props);
    }

    public RunnerConfig(Map<String, Object> props) {
        this(baseConfigDef(), props);
    }

    public static void main(String[] args) {
        System.out.println(baseConfigDef().toHtmlTable());
    }
}
