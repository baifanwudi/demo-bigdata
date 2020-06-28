package com.demo.analysis.spider;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.spider.BusCrawlData;
import com.demo.operation.BusCrawlFlatMapFunction;
import com.demo.util.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;

public class BusCrawlSpider extends AbstractReadDiffEnvTable implements Serializable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String logDate= DateUtils.nowDate();
        Dataset<Row> tableData = readTable(spark, args);
        Dataset<BusCrawlData> busCrawlData = tableData.flatMap(new BusCrawlFlatMapFunction(), Encoders.bean(BusCrawlData.class)).repartition(4);
        busCrawlData.createOrReplaceTempView("BusCrawlData");
        spark.sql("insert overwrite table tmp_trafficwisdom.crawl_bus_origin_data PARTITION(log_date ='" +logDate + "') select key,date,status,data,createTime from BusCrawlData");
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\bus_crawl_params.csv");
        tableData.show();
        tableData.printSchema();
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("select * from mid_trafficwisdom.crawl_bus_params").repartition(50);
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        BusCrawlSpider busCrawlSpider = new BusCrawlSpider();
        busCrawlSpider.runAll(args, true);
    }
}
