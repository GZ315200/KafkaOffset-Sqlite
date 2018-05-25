package com.unistack.tamboo.message.kafka.storage;

import com.unistack.tamboo.commons.utils.common.JdbcHelper;
import com.unistack.tamboo.message.kafka.bean.OffsetToMonitor;
import com.unistack.tamboo.message.kafka.callback.OffsetCallback;
import com.unistack.tamboo.message.kafka.exceptions.ConfigException;
import com.unistack.tamboo.message.kafka.queue.MonitorQueue;
import com.unistack.tamboo.message.kafka.runtime.RunnerConfig;
import com.unistack.tamboo.message.kafka.runtime.RuntimeConfig;
import com.unistack.tamboo.message.kafka.util.KafkaTableBaseLog;
import com.unistack.tamboo.message.kafka.util.Time;
import com.unistack.tamboo.message.kafka.util.TopicAdmin;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Gyges Zean
 * @date 2018/4/18
 * save the topic offset in sqlite on backing
 */
public class KafkaOffsetBackingStore implements OffsetBackingStore {

    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetBackingStore.class);

    private KafkaTableBaseLog<byte[], byte[]> offsetLog;

    private ScheduledExecutorService commitExecutorService;

    @Override
    public void start() {
        log.info("Starting KafkaOffsetBackingStore");
        offsetLog.start();
        log.info("Finished reading offsets topic and starting KafkaOffsetBackingStore");
    }

    @Override
    public void stop() {
        log.info("Stopping KafkaOffsetBackingStore");
        offsetLog.stop();
        close(30000);
        log.info("Finished Stop KafkaOffsetBackingStore");
    }


    /**
     * get offset by topic & timestamp.
     *
     * @param topic
     * @param timestamp
     * @return
     */
    public Map<TopicPartition, OffsetAndTimestamp> getOffset(String topic, Long timestamp) {
        return offsetLog.seekToOffsetForTimestamp(topic, timestamp);
    }


    @Override
    public void configure(RuntimeConfig config) throws Exception {
        String table = config.getString(RunnerConfig.OFFSET_STORAGE_TABLE);
        String topic = config.getString(RunnerConfig.USER_ANALYSIS_TOPIC);
        int batchSize = config.getInt(RuntimeConfig.INSERT_BATCH_SIZE);
        String className = config.getString(RuntimeConfig.CLASSNAME);
        String jdbcUrl = config.getString(RuntimeConfig.URL);
        String username = config.getString(RuntimeConfig.USERNAME);
        String password = config.getString(RuntimeConfig.PASSWORD);

        long commitIntervalMs = config.getLong(RuntimeConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG);

        JdbcHelper s = null;

        s = JdbcHelper.define()
                .settings("shared_cache", "true")
                .setClassName(className)
                .setConnection(jdbcUrl, username, password)
                .setTransactionIsolation(1)
                .setAutoCommit(false)
                .createStatement()
                .build();


        if (StringUtils.isBlank(table)) {
            throw new ConfigException("Offset storage table name must be specified");
        }
        if (StringUtils.isBlank(topic)) {
            throw new ConfigException("User data analysis topic name must be specified");
        }

        if (StringUtils.isBlank(jdbcUrl)) {
            throw new ConfigException("database's url must be specified");
        }

        this.commitExecutorService = Executors.newSingleThreadScheduledExecutor();
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.putAll(config.originals());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        Map<String, Object> adminProps = new HashMap<>(config.originals());

        NewTopic topicDescription = TopicAdmin.defineTopic(topic)
                .compacted()
                .partitions(config.getInt(RunnerConfig.USER_TOPIC_PARTITIONS_CONFIG))
                .replicationFactor(config.getShort(RunnerConfig.USER_TOPIC_REPLICA_CONFIG))
                .build();

        offsetLog = createKafkaBaseLog(table, topic, consumerProps,
                adminProps, topicDescription, batchSize, s);

//        启动定时去提交offset
        schedule(commitIntervalMs);
    }


    public void close(long timeoutMs) {
        commitExecutorService.shutdown();
        try {
            if (!commitExecutorService.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS))
                log.error("Graceful shutdown of offset commitOffsets thread timed out.");
        } catch (InterruptedException e) {
            // ignore and allow to exit immediately
        }
    }


    private void schedule(long commitIntervalMs) {
        commitExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                offsetLog.realCommit(new OffsetCallback() {
                    @Override
                    public void onCompletion(Throwable error, OffsetToMonitor result) {
                        Throwable cause = error.getCause();
                        if (cause instanceof Exception)
                            log.error("Failed to commit the offset.", error);
                        try {
                            MonitorQueue.put(result);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        log.info("callback==>topic:{},offset_size:{},memory:{}", result.getTopic(), result.getOffsetSize(), result.getMemory());
                    }
                });
            }
        }, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
    }


    private KafkaTableBaseLog<byte[], byte[]> createKafkaBaseLog(String table, String topic, Map<String, Object> consumerProps,
                                                                 final Map<String, Object> adminProps,
                                                                 final NewTopic topicDescription,
                                                                 int batchSize, JdbcHelper s) throws Exception {
//        自启动线程去创建table
        Runnable runnable = () -> {
            log.debug("Creating admin client to manage Runner internal offset topic.");

            try (TopicAdmin admin = new TopicAdmin(adminProps)) {
                Set<String> topics = admin.listAllTopics();
                if (topics.contains(topic)) {
                    log.error("topic{} has existed in brokers.", topic);
                    throw new TopicExistsException("topic has existed in brokers.");
                }
                Set<String> topicNames = admin.createTopics(topicDescription);
                if (topicNames.size() > 0)
                    log.info("topic(s) {} has been created automatic.", topicNames.toString());

            } catch (Exception e) {
                log.error("", e);
            }

        };
        return new KafkaTableBaseLog<>(
                consumerProps, topic, Time.SYSTEM,
                runnable, table, batchSize, s);
    }


}

