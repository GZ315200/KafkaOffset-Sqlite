package com.unistack.tamboo.message.kafka.bean;

import com.google.common.base.MoreObjects;

/**
 * @author Gyges Zean
 * @date 2018/5/4
 * the detail of the log dir, include message size,offset lag.
 */
public class LogDirDescription {


    private Integer brokerId;

    private TopicDetail topicDetail;


    public LogDirDescription() {
    }

    public LogDirDescription(Integer brokerId, TopicDetail topicDetail) {
        this.brokerId = brokerId;
        this.topicDetail = topicDetail;
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Integer brokerId) {
        this.brokerId = brokerId;
    }

    public TopicDetail getTopicDetail() {
        return topicDetail;
    }

    public void setTopicDetail(TopicDetail topicDetail) {
        this.topicDetail = topicDetail;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("brokerId", brokerId)
                .add("topicDetail", topicDetail)
                .toString();
    }

    public class TopicDetail {

        private String topic;

        private int partition;

        private long size;

        private long offsetLag;

        private boolean isFuture;

        public TopicDetail(String topic, int partition, long size, long offsetLag, boolean isFuture) {
            this.topic = topic;
            this.partition = partition;
            this.size = size;
            this.offsetLag = offsetLag;
            this.isFuture = isFuture;
        }

        public TopicDetail() {
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getParition() {
            return partition;
        }

        public void setParition(int partition) {
            this.partition = partition;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public long getOffsetLag() {
            return offsetLag;
        }

        public void setOffsetLag(long offsetLag) {
            this.offsetLag = offsetLag;
        }

        public boolean isFuture() {
            return isFuture;
        }

        public void setFuture(boolean future) {
            isFuture = future;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("topic", topic)
                    .add("partition", partition)
                    .add("size", size)
                    .add("offsetLag", offsetLag)
                    .add("isFuture", isFuture)
                    .toString();
        }
    }

}
