package com.demo.analysis.stats;

import com.demo.base.AbstractSparkSql;
import com.demo.common.ColumnUtil;
import org.apache.spark.sql.*;
import scala.collection.Seq;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

/**
 * @author allen.bai
 */
public class RouteCityInfo extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        executeHive(spark);
    }

    private void executeHive(SparkSession spark) {
        Dataset<Row> tableData = onlineTable(spark);
        tableData.cache();
        Dataset<Row> baseTable = tableData.withColumn("all_time", col("first_run_time").plus(col("second_run_time")).plus(col("sleep_time")));
        //线路平均时间
        Dataset<Row> avgCityTime = baseTable.groupBy("start_city_id", "transfer_city_id", "end_city_id", "first_traffic_type", "second_traffic_type")
                .agg(ceil(sum(col("all_time").multiply(col("line_num"))).divide(sum(col("line_num")))).as("avg_time"));
        //线路最小时间
        Dataset<Row> minCityTime = baseTable.groupBy("start_city_id", "transfer_city_id", "end_city_id", "first_traffic_type", "second_traffic_type")
                .agg(min(col("all_time")).as("min_time"));
        Seq<String> seq = ColumnUtil.columnNames("start_city_id,transfer_city_id,end_city_id,first_traffic_type,second_traffic_type");
        Dataset<Row> finalResult = avgCityTime.join(minCityTime, seq, "outer");
        finalResult.createOrReplaceTempView("routeCityInfo");
        spark.sql("insert overwrite table mid_trafficwisdom.route_city_info select start_city_id,transfer_city_id,end_city_id," +
                "first_traffic_type,second_traffic_type,avg_time ,min_time from routeCityInfo ");
        tableData.unpersist();
    }

    private Dataset<Row> onlineTable(SparkSession spark) {
        Dataset<Row> tableData = spark.sql("select * from app_trafficwisdom.route_traffic_transfer_without_date");
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        RouteCityInfo routeCityInfo = new RouteCityInfo();
        routeCityInfo.runAll(args, true);
    }
}
