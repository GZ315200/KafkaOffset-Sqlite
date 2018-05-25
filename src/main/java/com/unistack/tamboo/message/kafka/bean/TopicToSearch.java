package com.unistack.tamboo.message.kafka.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Gyges Zean
 * @date 2018/4/28
 *
 * this entity used when save topic's offset,partition,topic name,metadata,timestamp.
 */
public class TopicToSearch implements Serializable {

    @JsonProperty(value = "topic")
    private String topic;

    @JsonProperty(value = "offset")
    private long offset;

    @JsonProperty(value = "value")
    private String value;

    @JsonProperty(value = "partition")
    private long partition;

    @JsonProperty(value = "timestamp")
    private long timestamp;

    public TopicToSearch() {
    }

    public TopicToSearch(String topic, long offset, String value,
                         long partition, long timestamp) {
        this.topic = topic;
        this.offset = offset;
        this.value = value;
        this.partition = partition;
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public long getPartition() {
        return partition;
    }

    public void setPartition(long partition) {
        this.partition = partition;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

}
