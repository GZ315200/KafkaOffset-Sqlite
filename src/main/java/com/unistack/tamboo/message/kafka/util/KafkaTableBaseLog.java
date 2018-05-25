package com.unistack.tamboo.message.kafka.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unistack.tamboo.commons.utils.common.Const;
import com.unistack.tamboo.commons.utils.common.Converter;
import com.unistack.tamboo.commons.utils.common.JdbcHelper;
import com.unistack.tamboo.message.kafka.bean.OffsetToMonitor;
import com.unistack.tamboo.message.kafka.bean.TopicToSearch;
import com.unistack.tamboo.message.kafka.callback.OffsetCallback;
import com.unistack.tamboo.message.kafka.exceptions.ConnectException;
import com.unistack.tamboo.message.kafka.exceptions.KafkaException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public class KafkaTableBaseLog<K, V> extends AbstractKafkaLog<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaTableBaseLog.class);

    private String table;

    private int batchSize;

    private List<TopicToSearch> computeCache = Lists.newArrayList();


    private long started;

    //   create  a instance for JdbcHelper.
    private JdbcHelper s;

    public KafkaTableBaseLog(Map<String, Object> consumerConfigs, String topic,
                             Time time, Runnable initializer,
                             String table, int batchSize, JdbcHelper s) {
        super(consumerConfigs, topic, time, initializer);
        this.table = table;
        this.batchSize = batchSize;
        this.s = s;

    }


    public KafkaTableBaseLog(Map<String, Object> consumerConfigs, String topic,
                             Time time) {
        super(consumerConfigs, topic, time, null);
    }

    /**
     * @param topic
     * @param timestamp
     * @return
     */
    public Map<TopicPartition, OffsetAndTimestamp> seekToOffsetForTimestamp(String topic, Long timestamp) {
        consumer = createConsumer();
        List<PartitionInfo> partitionInfos = null;
        Map<TopicPartition, Long> data = Maps.newHashMap();
        started = time.milliseconds();
        while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
            partitionInfos = consumer.partitionsFor(topic);
            Utils.sleep(Math.min(time.milliseconds() - started, 1000));
        }
        if (partitionInfos == null)
            throw new ConnectException("Could not look up partition metadata for offset backing store topic in" +
                    " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if" +
                    " this is your first use of the topic it may have taken too long to create.");
        partitionInfos.forEach(partitionInfo -> {
            data.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), timestamp);
        });

        return consumer.offsetsForTimes(data);
    }


    /**
     * start to raise the kafka offset in backing
     */
    public void start() {
        logger.info("-------------------------------------------------------");
        logger.info("|            Start KafkaTableBaseLog with topic {}     |", topic);
        logger.info("-------------------------------------------------------");
        initializer.run();
        consumer = createConsumer();
        // We expect that the topics will have been created either manually by the user or automatically by the herder
        List<PartitionInfo> partitionInfos = null;

        List<TopicPartition> partitions = new ArrayList<>();
        started = time.milliseconds();
        while (partitionInfos == null && time.milliseconds() - started < CREATE_TOPIC_TIMEOUT_MS) {
            partitionInfos = consumer.partitionsFor(topic);
            Utils.sleep(Math.min(time.milliseconds() - started, 1000));
        }
        if (partitionInfos == null)
            throw new ConnectException("Could not look up partition metadata for offset backing store topic in" +
                    " allotted period. This could indicate a connectivity issue, unavailable topic partitions, or if" +
                    " this is your first use of the topic it may have taken too long to create.");

        for (PartitionInfo partition : partitionInfos)
            partitions.add(new TopicPartition(partition.topic(), partition.partition()));
        consumer.assign(partitions);

        readToLogEnd();

        consumer.subscribe(Collections.singletonList(topic));
        thread = new RunnerThread();
        thread.start();

        log.info("Finished reading KafkaBasedLog for topic " + topic);
        log.info("Started KafkaBasedLog for topic " + topic);
    }


    /**
     * write offset into table.
     */
    public void writeToTable(ConsumerRecord<K, V> record) {
        try {
            s.s.addBatch("insert into " + table + " values(" + record.topic() + ","
                    + record.offset() + "," + record.timestamp() + ")");
            getList(record.offset());
        } catch (SQLException e) {
            logger.error("Failed to add batch sql.", e);
        }
    }


    public void poll(long timeoutMs) {
        try {
            ConsumerRecords<K, V> records = consumer.poll(timeoutMs);
            for (ConsumerRecord<K, V> record : records) {
                if (record != null && record.value() != null) {
//                超过15分钟则不收集数据
                    if (record.timestamp() - started > Const.OFFSET_COLLECT_TIME)
                        break;
                    writeToTable(record);
                } else {
                    log.info("Consumer has been shutdown");
                }
            }
        } catch (WakeupException e) {
            // Expected on get() or stop(). The calling code should handle this
            throw e;
        } catch (KafkaException e) {
            logger.error("Error polling: " + e);
        }
    }


    /**
     * real commit
     */
    public void realCommit(OffsetCallback callback) {
        if (computeCache.size() > batchSize) {
            executeBatch();
            commit();
            consumer.commitSync();
//      if commit success then package the offset monitor data and add them to callback
            int b = Converter.toBytes(computeCache).length;
            OffsetToMonitor monitor = OffsetToMonitor
                    .defineForTopic(topic)
                    .memory(computeMemory(b))
                    .totalSize(totalSize())
                    .offsetSize(computeCache.size())
                    .timestamp()
                    .build();
            callback.onCompletion(null, monitor);
        }
    }


    /**
     * 获取offset的总数
     *
     * @return
     */
    private Long totalSize() {
        if (!endOffsets.isEmpty()) {
            for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet())
                return endOffsets.get(entry.getKey());
        }
        return 0L;
    }


    /**
     * 1 KB = 1024 bit = 128 Byte(字节)
     *
     * @param b
     * @return
     */
    private static String computeMemory(int b) {
        double kb = b / 128.00;
        BigDecimal bigDecimal = new BigDecimal(kb);
        BigDecimal decimal = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP);
        return decimal.toString();
    }


    /**
     * package the topic to list
     *
     * @param offset
     * @return
     */
    private List<TopicToSearch> getList(long offset) {
        TopicToSearch toSearch = new TopicToSearch();
        toSearch.setOffset(offset);
        computeCache.add(toSearch);
        return computeCache;
    }


    /**
     * execute the batch insert.
     */
    private void executeBatch() {
        try {
            s.s.executeBatch();
        } catch (SQLException e) {
            logger.error("Failed to execute batch method.", e);
        }
    }

    /**
     * execute the real commit.
     */
    private void commit() {
        try {
            s.c.commit();
        } catch (SQLException e) {
            logger.error("Failed to commit batch insert.", e);
        }
    }


    public void stop() {
        logger.info("Stopping KafkaTableBaseLog for topic " + topic);

        synchronized (this) {
            stopRequested = true;
        }
//        终止consumer长时间轮询
        consumer.wakeup();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new ConnectException("Failed to stop KafkaTableBaseLog. Exiting without cleanly shutting " +
                    "down it's producer and consumer.", e);
        }
        try {
            consumer.close();
        } catch (KafkaException e) {
            logger.error("Failed to stop KafkaTableBaseLog consumer", e);
        }

        logger.info("Stopped KafkaTableBaseLog for topic " + topic);
    }


    private class RunnerThread extends Thread {
        public RunnerThread() {
            super("KafkaTableLog Runner Thread - " + topic);
        }

        @Override
        public void run() {
            try {
                log.trace("{} started execution", this);
                while (true) {
                    synchronized (KafkaTableBaseLog.this) {
                        if (stopRequested)
                            break;
                    }
                    try {
                        poll(Integer.MAX_VALUE);
                    } catch (WakeupException e) {
                        // See previous comment, both possible causes of this wakeup are handled by starting this loop again
                        continue;
                    }
                }
            } catch (Throwable t) {
                log.error("Unexpected exception in {}", this, t);
            }
        }
    }


//    public static void main(String[] args) {
////
////         ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
////
////        Map<String, Object> consumerConfigs = new HashMap<>();
////        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaTable");
////        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.101:9092");
////        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
////        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
////        KafkaTableBaseLog topicBaseLog = new KafkaTableBaseLog(consumerConfigs, "test", Time.SYSTEM);
////
////        Runnable runnable = new Runnable() {
////            @Override
////            public void run() {
////                System.out.println(topicBaseLog.seekToOffsetForTimestamp("zean", System.currentTimeMillis()/1000).toString());
////            }
////        };
////
////        scheduler.scheduleAtFixedRate(runnable,1,2, TimeUnit.SECONDS);
////
////    }
//
//    }

}