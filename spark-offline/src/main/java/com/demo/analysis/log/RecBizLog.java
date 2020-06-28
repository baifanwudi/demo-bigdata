package com.demo.analysis.log;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.util.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import  static org.apache.spark.sql.functions.*;
import java.io.IOException;

public class RecBizLog extends AbstractReadDiffEnvTable {

    public static final String REC_BIZ_LOG_PATH = "viewfs:///data/twms/traffichuixing/rec_biz_log_product";

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String date = DateUtils.produceDateOrYesterday(args);
        Dataset<Row> tableData= readTable(spark,args);

        Dataset<Row> recBizLog=tableData.withColumn("category_struct",explode(col("categoryRecStopReasonList")))
                .withColumn("candidateCategories_str",concat_ws(",",col("candidateCategories")))
                .withColumn("originCategories_str",concat_ws(",",col("originCategories")))
                .withColumn("source_str",concat_ws(",",col("source")))
                .drop("categoryRecStopReasonList","candidateCategories","originCategories","source").repartition(1);
        recBizLog.createOrReplaceTempView("recBizLog");
        String sql="insert overwrite table tmp_trafficwisdom.rec_biz_log PARTITION(log_date ='" + date + "' )  select " +
                "traceId as trace_id,unionId as union_id,planCode as plan_code,platId as plat_id,beginDate as begin_date,fromCityId as from_city_id," +
                "fromCityName as from_city_name,fromId as from_id,fromStation as from_station,toCityId as to_city_id,toCityName as to_city_name," +
                "toId as to_id,toStation as to_station,source_str as source,recommendPosition as recommend_position,originCategories_str as origin_categories," +
                "candidateCategories_str as candidate_categories,recCategory as rec_category,category_struct.category as no_category," +
                "category_struct.msg as no_msg,category_struct.code as no_code,requestTime as request_time, observeGroup as ob_group from recBizLog ";
        logger.info("execute the sql is :"+sql);
        spark.sql(sql);
    }

    public static void main(String[] args) throws IOException {
        RecBizLog recBizLog =new RecBizLog();
        recBizLog.runAll(args,true);
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData=spark.read().json("/Users/allenbai/Downloads/data/json/rec_biz_log.json");
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        String date = DateUtils.produceDateOrYesterday(args);
        String path=REC_BIZ_LOG_PATH+"/"+date;
        Dataset<Row> tableData = spark.read().json(path).distinct();
        return tableData;
    }
}
