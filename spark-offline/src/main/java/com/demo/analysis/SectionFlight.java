package com.demo.analysis;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.util.DateUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SectionFlight extends AbstractReadDiffEnvTable implements Serializable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args);
        StructType structType = new StructType()
                .add("transport_code", DataTypes.StringType, false)
                .add("start_station_code", DataTypes.StringType, false)
                .add("end_station_code", DataTypes.StringType, false)
                .add("start_time", DataTypes.StringType, false)
                .add("end_time", DataTypes.StringType, false)
                .add("run_time", DataTypes.IntegerType, false)
                .add("duration_date", DataTypes.StringType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
        Dataset<Row> flightSection = tableData.flatMap((FlatMapFunction<Row, Row>) row -> {
            List<Row> rowList = new ArrayList<Row>();
            try {
                String transportCode = row.<String>getAs("transport_code").trim();
                String startStationCode = row.<String>getAs("start_station_code").trim();
                String endStationCode = row.<String>getAs("end_station_code").trim();
                String startTime = row.<String>getAs("start_time").trim();
                String endTime = row.<String>getAs("end_time").trim();
                String planStartDate = row.<String>getAs("plan_start_date").trim();
                String planEndDate = row.<String>getAs("plan_end_date").trim();
                Integer runTime = row.<Integer>getAs("run_time");
                String[] strs = row.<String>getAs("schedule_week").trim().split(",");
                List<Integer> weeks = new ArrayList<Integer>();
                for (String tmp : strs) {
                    weeks.add(Integer.parseInt(tmp));
                }
                Date fromDate = DateUtils.parseStringToDate(planStartDate);
                Date toDate = DateUtils.parseStringToDate(planEndDate);
                int days = DateUtils.daysBetweenDates(toDate, fromDate);
                for (int i = 0; i <= days; i++) {
                    Date newData = DateUtils.dateAddDay(fromDate, i);
                    if (weeks.contains(DateUtils.getWeek(newData))) {
                        String durationData = DateUtils.dateToString(newData);
                        Row newRow = RowFactory.create(transportCode, startStationCode, endStationCode, startTime, endTime, runTime, durationData);
                        rowList.add(newRow);
                    }
                }
            }catch (Exception e){
                logger.error("row is :"+row+" ; the error reason is:"+e.getMessage(),e);
            }
            return rowList.iterator();
        }, encoder).repartition(1);

        flightSection.createOrReplaceTempView("flightSection");
        String sql="insert overwrite table mid_trafficwisdom.section_flight select transport_code,start_station_code,end_station_code,start_time,end_time,run_time,duration_date,current_date() as create_date" +
                " from flightSection";
        logger.info(sql);
        spark.sql(sql);

    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args){
        Dataset<Row> tableData=spark.sql("select * from mid_trafficwisdom.flight_base_info");
        return tableData;
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .option("delimiter", "#")
                .csv("D:\\data\\csv\\flight_base_info.csv")
                .selectExpr("transport_code", "start_station_code", "end_station_code", "schedule_week", "start_time", "end_time",
                        "substring(plan_start_date,1,10) as plan_start_date", "substring(plan_end_date,1,10) as plan_end_date", "run_time").limit(2);
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        SectionFlight sectionFlight = new SectionFlight();
        sectionFlight.runAll(args, true);
    }
}
