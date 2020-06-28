package com.demo.conf;

import com.demo.common.ConfigUtil;

public class RedisConfig {
    /**
     * 同程redis cache-name
     */
    public static final String REDIS_CACHE_NAME=ConfigUtil.getPros("redis.cache.name");
    /**
     * key保持的时间
     */
    public static final Integer REDIS_KEY_ALIVE=Integer.parseInt(ConfigUtil.getPros("redis.cache.period"));

}
