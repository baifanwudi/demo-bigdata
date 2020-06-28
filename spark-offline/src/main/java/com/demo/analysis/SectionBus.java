package com.demo.analysis;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.spider.BusSectionBO;
import com.demo.operation.BusSectionFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.io.Serializable;

public class SectionBus extends AbstractReadDiffEnvTable implements Serializable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args);
        Dataset<BusSectionBO> busSection = tableData.flatMap(new BusSectionFlatMapFunction(), Encoders.bean(BusSectionBO.class)).repartition(1);
        busSection.createOrReplaceTempView("BusSection");
        spark.sql("insert overwrite table mid_trafficwisdom.section_bus select busNo,durationDate,startCityId,startBusCityId,startBusCityName,startStationName," +
                "startTime,endCityId,endBusCityId,endBusCityName,endStationName,endTime,distance,runTime,ticketPrice,current_date() as create_date from BusSection ");
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read().json("D:\\data\\json\\crawl_bus_line_data.json");
        tableData.show(false);
        tableData.printSchema();
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT * FROM mid_trafficwisdom.crawl_bus_line_data ");
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        SectionBus trainSectionTable = new SectionBus();
        trainSectionTable.runAll(args, true);
    }
}
