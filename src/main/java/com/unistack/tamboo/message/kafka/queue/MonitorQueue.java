package com.unistack.tamboo.message.kafka.queue;

import com.unistack.tamboo.message.kafka.bean.OffsetToMonitor;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author Gyges Zean
 * @date 2018/5/24
 */
public class MonitorQueue {

    private static BlockingQueue<OffsetToMonitor> monitorQueue = new LinkedBlockingDeque<>();

    public static void put(OffsetToMonitor offsetToMonitor) throws InterruptedException {
        Objects.requireNonNull(offsetToMonitor, "Offset monitor info must be specified.");
        monitorQueue.put(offsetToMonitor);
    }

    public static OffsetToMonitor take() throws InterruptedException {
        return monitorQueue.take();
    }


}
