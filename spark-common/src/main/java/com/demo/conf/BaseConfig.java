package com.demo.conf;

import com.demo.common.ConfigUtil;

public class BaseConfig {
    /**
     *hdfs前缀
     */
    public static final String HDFS_PREFIX = ConfigUtil.getPros("hdfs.prefix");

}
