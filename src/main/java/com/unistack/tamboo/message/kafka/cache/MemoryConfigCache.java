package com.unistack.tamboo.message.kafka.cache;


import com.google.common.collect.Maps;
import com.unistack.tamboo.message.kafka.runtime.RuntimeConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Gyges Zean
 * @date 2018/5/15
 * 用于config的缓存
 */
public class MemoryConfigCache {


    private final ConcurrentHashMap<String, MemoryConfigCache.Cache<? extends Object>> cacheMap = new ConcurrentHashMap<>();


    public <C> MemoryConfigCache.Cache<C> createCache(String runnerName, Map<String, C> value) {
        MemoryConfigCache.Cache<C> cache = new MemoryConfigCache.Cache<C>(value);
        MemoryConfigCache.Cache<C> oldCache = (MemoryConfigCache.Cache<C>) cacheMap.putIfAbsent(runnerName, cache);
        return oldCache == null ? cache : oldCache;
    }


    /**
     * 根据runnerName获取缓存
     *
     * @param runnerName
     * @param <C>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <C> MemoryConfigCache.Cache<C> cache(String runnerName) {
        MemoryConfigCache.Cache<?> cache = cacheMap.get(runnerName);
        if (cache != null) {
            return (Cache<C>) cache;
        } else
            return null;
    }


    public static class Cache<C> {

        private final ConcurrentHashMap<String, C> config;

        public Cache(Map<String, C> properties) {
            this.config = new ConcurrentHashMap<>();
            this.config.putAll(properties);
        }

        public boolean contains(String runnerName) {
            return config.containsKey(runnerName);
        }

        public C get(String runnerName) {
            return config.get(runnerName);
        }

        public C put(String runnerName, C properties) {
            return config.put(runnerName, properties);
        }

        public C remove(String runnerName) {
            return config.remove(runnerName);
        }

    }


    public static void main(String[] args) {
        Map<String, Object> map = Maps.newHashMap();
        map.put(RuntimeConfig.RUNNER_NAME, "test");
        map.put(RuntimeConfig.INSERT_BATCH_SIZE, 10000);

        MemoryConfigCache memoryConfigCache = new MemoryConfigCache();
        memoryConfigCache.createCache("test", map);

        MemoryConfigCache.Cache cache = memoryConfigCache.cache("test");

//        System.out.println(cache.configMap("test"));
        System.out.println(cache.get(RuntimeConfig.INSERT_BATCH_SIZE));


    }
}
