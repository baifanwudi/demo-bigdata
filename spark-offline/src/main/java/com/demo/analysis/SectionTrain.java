package com.demo.analysis;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.spider.TrainSectionBO;
import com.demo.operation.TrainSectionFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;

public class SectionTrain extends AbstractReadDiffEnvTable implements Serializable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args).repartition(10);
        Dataset<TrainSectionBO> trainSection = tableData.flatMap(new TrainSectionFlatMapFunction(), Encoders.bean(TrainSectionBO.class)).repartition(5);
        trainSection.createOrReplaceTempView("trainSection");
        spark.sql("insert overwrite table mid_trafficwisdom.section_train select trainNo,transportCode,durationDate,beginId,beginStationName,beginTime," +
                "endId,endStationName,endTime,current_date() as create_date from trainSection ");
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .json("D:\\data\\json\\crawl_train_stopover_data.json")
                .selectExpr("data", "cast(date as string)", "key", "cast(train_code as string)", "cast(train_no as string)")
                .limit(1);
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT * FROM mid_trafficwisdom.crawl_train_stopover_data ");
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        SectionTrain sectionTrain = new SectionTrain();
        sectionTrain.runAll(args, true);
    }
}
