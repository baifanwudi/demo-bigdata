package com.demo.analysis.redis;

import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.redis.SeatTypeRank;
import com.demo.operation.TrainSeatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import redis.clients.jedis.Jedis;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class TrainSeatTypeRank extends AbstractReadDiffEnvTable {

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        Dataset<Row> tableData=readTable(spark,args);
        ExpressionEncoder<Row> rowEncoder=getEncoder();
        Dataset<Row> aggResult=   tableData.groupBy("first_traffic_type", "first_traffic_code","first_seat_type", "start_city_id", "start_city_name", "start_station_code", "start_station_name",
                "transfer_city_id", "transfer_city_name", "transfer_arrive_station_code", "transfer_arrive_station_name",
                "second_traffic_type", "second_traffic_code","second_seat_type", "transfer_leave_station_code", "transfer_leave_station_name",
                "end_city_id", "end_city_name", "end_station_code", "end_station_name").agg(sum("ticket_num").as("total_num"))
             .map(new TrainSeatMapFunction(),rowEncoder);

        WindowSpec w = Window.partitionBy("item_id").orderBy(col("total_num").desc_nulls_last());
        Dataset<Row> top2ItemId = aggResult.withColumn("rank", row_number().over(w)).where(col("rank").leq(2)).groupBy("item_id")
                .agg(collect_set(struct(("first_seat_type" ),"second_seat_type","total_num","rank")).as("combined_set"));
        saveRedis(top2ItemId);
    }

    private void saveRedis(Dataset<Row> dataset) {
        dataset.foreachPartition(
                data -> {
                    Jedis jedis = new Jedis("localhost");
                    while (data.hasNext()) {
                        Row record = data.next();
                        String key = String.format("seatType:%s", record.<String>getAs("item_id"));
                        WrappedArray<Row> combinedSet= record.<WrappedArray<Row>>getAs("combined_set");
                        List<Row> rowList = new ArrayList<Row>(JavaConversions.<Row>seqAsJavaList(combinedSet));
                        List<SeatTypeRank> seatTypeRankList=new ArrayList<SeatTypeRank>();
                        for (Row row : rowList) {
                            Integer firstSeatType = row.<Integer>getAs("first_seat_type");
                            Integer secondSeatType = row.<Integer>getAs("second_seat_type");
                            Long totalNum = row.<Long>getAs("total_num");
                            Integer rankNum = row.<Integer>getAs("rank");
                            SeatTypeRank seatTypeRank=SeatTypeRank.builder().firstSeatType(firstSeatType).secondSeatType(secondSeatType).totalNum(totalNum).rankNum(rankNum).build();
                            seatTypeRankList.add(seatTypeRank);
                        }
                        String value= JSONObject.toJSONString(seatTypeRankList);
                        if(isLocalEnvConfig()){
                            System.out.println("key:" + key + ",value:" + value);
                        }
                        jedis.set(key,  value);
                    }
                }
        );
    }

    private ExpressionEncoder<Row> getEncoder() {
        StructType structTypeMember = new StructType();
        structTypeMember = structTypeMember.add("item_id", DataTypes.StringType, true);
        structTypeMember = structTypeMember.add("first_seat_type", DataTypes.IntegerType, true);
        structTypeMember = structTypeMember.add("second_seat_type", DataTypes.IntegerType, true);
        structTypeMember = structTypeMember.add("total_num", DataTypes.LongType, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structTypeMember);
        return encoder;
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.sql("SELECT * FROM mid_trafficwisdom.wisdom_traffic_order_route where first_traffic_type='T' and second_traffic_type='T'");
        return tableData;
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args) {
        Dataset<Row> tableData = spark.read()
                .option("inferschema", "true")
                .option("header", "true")
                .option("encoding", "gbk")
                .csv("D:\\data\\csv\\wisdom_traffic_order_route.csv");
        tableData.printSchema();
        tableData.show(false);
        return tableData;
    }

    public static void main(String[] args) throws IOException {
        TrainSeatTypeRank trainSeatTypeRank=new TrainSeatTypeRank();
        trainSeatTypeRank.runAll(args,true,false);
    }
}
