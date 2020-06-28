package com.demo.analysis.stats;

import com.demo.base.AbstractSparkSql;
import com.demo.common.ColumnUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;
import java.io.IOException;
import static org.apache.spark.sql.functions.*;

public class SectionCityTime extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        executeHive(spark);
    }

    private void executeHive(SparkSession spark) {
        Dataset<Row> tableData = onlineTable(spark);
        Dataset<Row> sectionAvgTime = tableData.groupBy("start_city_id", "end_city_id", "traffic_type")
                .agg(ceil(sum(col("run_time").multiply(col("line_num"))).divide(sum(col("line_num")))).as("avg_time"));
        Dataset<Row> sectionMinTime = tableData.groupBy("start_city_id", "end_city_id", "traffic_type")
                .agg(min("run_time").as("min_time"));
        Dataset<Row> sectionMaxTime = tableData.groupBy("start_city_id", "end_city_id", "traffic_type")
                .agg(max("run_time").as("max_time"));

        Seq<String> seq = ColumnUtil.columnNames("start_city_id,end_city_id,traffic_type");
        Dataset<Row> finalResult = sectionAvgTime.join(sectionMinTime, seq, "outer").join(sectionMaxTime,seq,"outer").repartition(1);
        finalResult.createOrReplaceTempView("sectionCityTime");
        spark.sql("insert overwrite table mid_trafficwisdom.section_city_time select start_city_id,end_city_id,traffic_type,avg_time,min_time,max_time,current_date() from sectionCityTime");
    }

    private Dataset<Row> onlineTable(SparkSession spark) {
        Dataset<Row> tableData = spark.sql("select * from app_trafficwisdom.section_traffic_without_date where start_city_id!=end_city_id and run_time>0 and run_time is not null");
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        SectionCityTime sectionCityTime = new SectionCityTime();
        sectionCityTime.runAll(args, true);
    }
}
