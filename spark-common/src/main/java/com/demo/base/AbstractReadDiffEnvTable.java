package com.demo.base;

import com.demo.conf.CommonConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class AbstractReadDiffEnvTable extends AbstractSparkSql {

    public Dataset<Row> readTable(SparkSession spark,String[] args) {
        Dataset<Row> tableData;
        if (isLocalEnvConfig()) {
            tableData = offlineTable(spark,args);
        } else {
            tableData = onlineTable(spark,args);
        }
        return tableData;
    }

    protected abstract Dataset<Row> offlineTable(SparkSession spark,String[] args);

    protected abstract Dataset<Row> onlineTable(SparkSession spark,String[] args);

    protected static Boolean isLocalEnvConfig(){
        return CommonConfig.isLocalEnvConfig();
    }
}
