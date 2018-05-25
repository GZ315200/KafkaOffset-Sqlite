package com.unistack.tamboo.message.kafka.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.unistack.tamboo.commons.utils.common.Const.State;

/**
 * @author Gyges Zean
 * @date 2018/4/28
 * this class used when package the offset status.
 */
public class OffsetToMonitor {

    private static Logger log = LoggerFactory.getLogger(OffsetToMonitor.class);

    private String topic;

    @JsonProperty(value = "offset_size")
    private long offsetSize; //提交的offset数

    @JsonProperty(value = "total_size")
    private long totalSize; //总offset数

    @JsonProperty(value = "memory")
    private String memory; //占用内存大小

    private Long timestamp;

    public OffsetToMonitor(long offsetSize, long totalSize, String memory) {
        this.offsetSize = offsetSize;
        this.totalSize = totalSize;
        this.memory = memory;
    }

    public OffsetToMonitor(String topic, long offsetSize, long totalSize, String memory,Long timestamp) {
        this.topic = topic;
        this.offsetSize = offsetSize;
        this.totalSize = totalSize;
        this.memory = memory;
        this.timestamp = timestamp;
    }

    public static Logger getLog() {
        return log;
    }


    public String getTopic() {
        return topic;
    }

    public long getOffsetSize() {
        return offsetSize;
    }


    public long getTotalSize() {
        return totalSize;
    }

    public String getMemory() {
        return memory;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public static class MonitorBuilder {
        private String topic;

        private long offsetSize; //提交的offset数

        private long totalSize; //总offset数

        private String memory; //占用内存大小

        private Long timestamp;

        public MonitorBuilder(String topic) {
            this.topic = topic;
        }

        public MonitorBuilder offsetSize(long offsetSize) {
            this.offsetSize = offsetSize;
            return this;
        }

        public MonitorBuilder timestamp() {
            this.timestamp = System.currentTimeMillis();
            return this;
        }

        public MonitorBuilder totalSize(long totalSize) {
            this.totalSize = totalSize;
            return this;
        }

        public MonitorBuilder memory(String memory) {
            this.memory = (memory + "KB/s");
            return this;
        }

        public OffsetToMonitor build() {
            return new OffsetToMonitor(topic, offsetSize, totalSize, memory,timestamp);
        }
    }

    public static MonitorBuilder defineForTopic(String topic) {
        return new MonitorBuilder(topic);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("topic", topic)
                .add("offsetSize", offsetSize)
                .add("totalSize", totalSize)
                .add("memory", memory)
                .toString();
    }

    /**
     * @return
     */
    public String metrics() {
        ObjectMapper o = new ObjectMapper();
        try {
            return o.writeValueAsString(new OffsetToMonitor(offsetSize, totalSize, memory));
        } catch (JsonProcessingException e) {
            log.error("Failed to parse the OBJECT to JSON", e);
        }
        return State.FAILED.name();
    }


    public static void main(String[] args) {

        OffsetToMonitor monitor =
                OffsetToMonitor.defineForTopic("topic")
                        .memory("5")
                        .offsetSize(1232)
                        .totalSize(9999999).build();
        System.out.printf(monitor.getTopic());

    }

}
