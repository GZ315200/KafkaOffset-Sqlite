package com.unistack.tamboo.message.kafka.callback;

import com.unistack.tamboo.commons.utils.common.Callback;
import com.unistack.tamboo.message.kafka.bean.OffsetToMonitor;

/**
 * @author Gyges Zean
 * @date 2018/4/28
 */
public interface OffsetCallback extends Callback<OffsetToMonitor> {

    @Override
    void onCompletion(Throwable error, OffsetToMonitor result);
}
