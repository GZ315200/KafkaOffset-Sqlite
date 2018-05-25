package com.unistack.tamboo.message.kafka.io;

import com.google.common.collect.Maps;
import com.unistack.tamboo.commons.utils.common.JdbcHelper;
import com.unistack.tamboo.message.kafka.cache.MemoryConfigCache;
import com.unistack.tamboo.message.kafka.runtime.RuntimeConfig;
import com.unistack.tamboo.message.kafka.runtime.entities.OffsetInfo;
import com.unistack.tamboo.message.kafka.storage.KafkaOffsetBackingStore;
import com.unistack.tamboo.message.kafka.util.SystemTime;
import com.unistack.tamboo.message.kafka.util.Time;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 */
public class OffsetStorageReaderImpl implements OffsetStorageReader {

    public static final Logger log = LoggerFactory.getLogger(OffsetStorageReaderImpl.class);

    private Time time = SystemTime.SYSTEM;

    private MemoryConfigCache configCache = new MemoryConfigCache();

    private KafkaOffsetBackingStore kafkaOffsetBackingStore = new KafkaOffsetBackingStore();


    @Override
    public <T> Map<String, Object> offset(String runner, long span) {
        long now = time.milliseconds();//current time.
        long previous = now - span;//previous time.

        Map<String, Object> result = Maps.newHashMap();

        MemoryConfigCache.Cache cache = configCache.cache(runner);

        String table = (String) cache.get(RuntimeConfig.OFFSET_STORAGE_TABLE);
        String table_path = (String) cache.get(RuntimeConfig.OFFSET_STORAGE_TABLE_PATH);

        JdbcHelper helper = JdbcHelper.
                define()
                .setAutoCommit(true)
                .setClassName()
                .setConnection(table_path + table)
                .createStatement()
                .build();
        try {
            helper.s.executeUpdate("SELECT t.topic,t.partition,t.offset,t.value FROM "
                    + table + " t where t.timestamp >=" + previous + " t.timestamp =< " + now);
            ResultSet rs = helper.s.getResultSet();
            while (rs.next()) {
                OffsetInfo offsetInfo = new OffsetInfo();
                offsetInfo.setOffset(rs.getInt("offset"));
                offsetInfo.setPartition(rs.getInt("partition"));
                offsetInfo.setTopic(rs.getString("topic"));
                offsetInfo.setValue(rs.getString("value"));
                offsetInfo.setTimestamp(rs.getLong("timestamp"));
                result.put(offsetInfo.getTopic(), offsetInfo);
            }
        } catch (SQLException e) {
            log.error("Failed to execute the sql.", e);
        }
        return result;
    }


    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsets(String topic, long timestamp) {
        return kafkaOffsetBackingStore.getOffset(topic, timestamp);
    }


}
