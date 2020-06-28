package com.demo.analysis.stats;

import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.util.MapDistanceUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * 火车临近站点计算
 */
public class StationCrossDistance extends AbstractReadDiffEnvTable {

    public static void main(String[] args) throws IOException {
        StationCrossDistance crossDistance = new StationCrossDistance();
        crossDistance.runAll(args, true);
    }

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData = readTable(spark,args).cache();
        Dataset<Row> tableA = tableData.selectExpr("city_id as start_city_id","city_name as start_city_name","station_code as start_station_code", "station_name as start_station_name",
                "traffic_type as start_traffic_type", "lng as start_lng", "lat as start_lat");
        Dataset<Row> tableB = tableData.selectExpr("city_id as end_city_id","city_name as end_city_name","station_code as end_station_code", "station_name as end_station_name","traffic_type as end_traffic_type"
                ,"lng as end_lng", "lat as end_lat");
        //交叉join,且过滤同类型同站
        Dataset<Row> crossTableData = tableA.crossJoin(tableB);

        StructType structType = new StructType()
                .add("start_city_id", DataTypes.IntegerType, false)
                .add("start_city_name", DataTypes.StringType, false)
                .add("start_station_code", DataTypes.StringType, false)
                .add("start_station_name", DataTypes.StringType, false)
                .add("start_traffic_type", DataTypes.StringType, false)
                .add("end_city_id", DataTypes.IntegerType, false)
                .add("end_city_name", DataTypes.StringType, false)
                .add("end_station_code", DataTypes.StringType, false)
                .add("end_station_name", DataTypes.StringType, false)
                .add("end_traffic_type", DataTypes.StringType, false)
                .add("distance", DataTypes.DoubleType, false);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
        Dataset<Row> stationDistance = crossTableData.map((MapFunction<Row, Row>) row -> {
            double startEndDistance = MapDistanceUtils.getDistance(row.<BigDecimal >getAs("start_lat").doubleValue(), row.<BigDecimal >getAs("start_lng").doubleValue(),
                    row.<BigDecimal >getAs("end_lat").doubleValue(), row.<BigDecimal>getAs("end_lng").doubleValue());
            return RowFactory.create(
                    row.<Integer>getAs("start_city_id"), row.<String>getAs("start_city_name"), row.<String>getAs("start_station_code"), row.<String>getAs("start_station_name"), row.<String>getAs("start_traffic_type"),
                    row.<Integer>getAs("end_city_id"), row.<String>getAs("end_city_name"), row.<String>getAs("end_station_code"), row.<String>getAs("end_station_name"), row.<String>getAs("end_traffic_type"),
                    startEndDistance);
        }, encoder).repartition(1);

        stationDistance.createOrReplaceTempView("stationDistance");
        String sql="insert overwrite table mid_trafficwisdom.station_distance select start_city_id,start_city_name,start_station_code,start_station_name,start_traffic_type,end_city_id,end_city_name," +
                "end_station_code,end_station_name,end_traffic_type,distance,current_date() as create_date from stationDistance where distance!=0";
        logger.info(sql);
        spark.sql(sql);
        tableData.unpersist();
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\station_lng_lat.csv").limit(5);
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        String sql="SELECT * FROM mid_trafficwisdom.station_lng_lat";
        logger.info(sql);
        Dataset<Row> tableData = spark.sql(sql);
        return tableData;
    }
}
