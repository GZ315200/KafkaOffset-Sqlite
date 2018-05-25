package com.unistack.tamboo.message.kafka.io;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/4/23
 * <p>
 * OffsetStorageReader provides access to the offset storage used by sources. This can be used by
 * runner to determine offsets to start consuming data from. This is most commonly used during
 * initialization of a runner, but can also be used during runtime, e.g. when reconfiguring a task.
 */
public interface OffsetStorageReader {

    /**
     * Get the offset for the specified runner & span.
     *
     * @return object uniquely identifying the offset in the partition of data
     */
    <T> Map<String, Object> offset(String runner, long span);

    /**
     * get the offset by topic & timestamp.
     * @param topic
     * @param timestamp
     * @return
     */
    Map<TopicPartition, OffsetAndTimestamp> offsets(String topic, long timestamp);
}
