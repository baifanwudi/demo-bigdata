package com.demo.analysis.stats;

import com.demo.base.AbstractSparkSql;
import com.demo.common.ColumnUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class SectionCityPrice extends AbstractSparkSql {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        executeHive(spark);
    }

    private void executeHive(SparkSession spark) {
        Dataset<Row> tableData = onlineTable(spark);
        Dataset<Row> sectionAvgPrice = tableData.groupBy("start_city_id", "end_city_id", "traffic_type")
                .agg(ceil(sum(col("price").multiply(col("line_num"))).divide(sum(col("line_num")))).as("avg_price"));
        Dataset<Row> sectionMinPrice = tableData.groupBy("start_city_id", "end_city_id", "traffic_type").agg(min("price").as("min_price"));
        Dataset<Row> sectionMaxPrice=tableData.groupBy("start_city_id", "end_city_id", "traffic_type").agg(max("price").as("max_price"));

        Seq<String> seq = ColumnUtil.columnNames("start_city_id,end_city_id,traffic_type");
        Dataset<Row> sectionPriceInfo = sectionAvgPrice.join(sectionMinPrice, seq, "outer").join(sectionMaxPrice,seq,"outer").repartition(1);
        sectionPriceInfo.createOrReplaceTempView("sectionCityPrice");
        String sql="insert overwrite table mid_trafficwisdom.section_city_price select start_city_id,end_city_id,traffic_type,avg_price,min_price,max_price,current_date() from sectionCityPrice";
        logger.info(sql);
        spark.sql(sql);
    }

    private Dataset<Row> onlineTable(SparkSession spark) {
        String sql="select * from app_trafficwisdom.section_traffic_without_date where start_city_id!=end_city_id and  price>0 and price is not null";
        logger.info(sql);
        Dataset<Row> tableData = spark.sql(sql);
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        SectionCityPrice sectionCityPrice = new SectionCityPrice();
        sectionCityPrice.runAll(args, true);
    }
}
