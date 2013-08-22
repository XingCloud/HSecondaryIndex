package com.xingcloud.xa.secondaryindex.utils;

/**
 * User: IvyTang
 * Date: 12-11-28
 * Time: 下午1:55
 */
public class Constants {

    public static final String TIMEZONE = "GMT+8";

    public static final String COLUMN_FAMILY = "val";

    public static final String HBASE_PORT = "3181";

    public static final int EXECUTOR_THREAD_COUNT = 20;

    public static final String PROPERTY_TABLE_PREFIX = "properties_";
    public static final String INDEX_TABLE_SUFFIX = "_index";

    public static final String OPERATION_PUT = "put";
    public static final String OPERATION_DELETE = "delete";

    public static final String SECONDARY_INDEX_LOG_DIR = "/data0/log/secondaryindex";

    public static final byte[] ZERO = {0};
}
