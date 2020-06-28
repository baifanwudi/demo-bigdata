package com.demo.analysis.spider;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.demo.base.AbstractReadDiffEnvTable;
import com.demo.bean.spider.CrawlData;
import com.demo.util.DateUtils;
import com.demo.util.EncodeUtils;
import com.demo.util.HttpUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TrainCrawlSpider extends AbstractReadDiffEnvTable implements Serializable {

    public static DateTimeFormatter DAY_HOUR_MIN_SEC_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void executeProgram(String[] args, SparkSession spark) throws IOException {
        String logDate= DateUtils.nowDate();
        Dataset<Row> tableData = readTable( spark,args);
        Dataset<CrawlData > trainCrawlData = tableData.map((MapFunction<Row, CrawlData>) obj -> {
            CrawlData crawlData=new CrawlData();
            String createTime=LocalDateTime.now().format(DAY_HOUR_MIN_SEC_PATTERN);
            crawlData.setCreateTime(createTime);
            try {
                String fromStationCode = obj.<String>getAs("from_station_code") == null ? "" : obj.<String>getAs("from_station_code");
                String toStationCode = obj.<String>getAs("to_station_code") == null ? "" : obj.<String>getAs("to_station_code");
                String trainCode = obj.<String>getAs("transport_code") == null ? "" : obj.<String>getAs("transport_code");
                String date = obj.<String>getAs("start_date") == null ? "" : obj.<String>getAs("start_date");
                String requestData = getTrainStopoversInfo(trainCode, fromStationCode, toStationCode, date);
                JSONObject jsonObject = JSONObject.parseObject(requestData);
                String trainNo=jsonObject.getString("train_no");
                String key=trainCode+"@"+date+"@"+fromStationCode+"@"+toStationCode;
                crawlData.setKey(key);
                crawlData.setTrainNo(trainNo);
                crawlData.setTrainCode(trainCode);
                crawlData.setDate(date);

                if (jsonObject != null && jsonObject.containsKey("success") && jsonObject.getBoolean("success") && jsonObject.containsKey("data")) {
                    JSONArray jsonArray = jsonObject.getJSONArray("data");
                    crawlData.setStatus("success");
                    crawlData.setData(jsonArray.toJSONString());
                    return crawlData;
                } else {
                    crawlData.setStatus("fail");
                    crawlData.setData("查询" + obj.toString() + "失败,无返回结果");
                    return crawlData;
                }
            } catch (Exception e) {
                crawlData.setStatus("system-error");
                crawlData.setData(e.getMessage());
                logger.error("查询" + obj.toString() + "失败，操作异常"+e.getMessage(),e);
                return crawlData;
            }
        }, Encoders.bean(CrawlData.class));

        trainCrawlData.createOrReplaceTempView("TrainCrawlData");
        spark.sql("insert overwrite table tmp_trafficwisdom.crawl_train_origin_data PARTITION(log_date ='" +logDate + "') select key,trainNo,trainCode,date,status,data,createTime from TrainCrawlData");
    }

    @Override
    protected Dataset<Row> onlineTable(SparkSession spark, String[] args){
        Dataset<Row> tableData = spark.sql("select * from mid_trafficwisdom.crawl_train_params_date ").repartition(200);
        return tableData;
    }

    @Override
    protected Dataset<Row> offlineTable(SparkSession spark, String[] args){
        Dataset<Row> tableData = spark.read()
                .option("header", "true")
                .option("encoding", "gbk")
                .schema(tableSchema())
                .csv("D:\\data\\csv\\route_train_start_end_base_day.csv");
        tableData.show();
        tableData.printSchema();
        return tableData;
    }

    private StructType tableSchema(){
        List<StructField> inputFields=new ArrayList<>();
        String splitSeq=",";
        String stringType="transport_code,start_station_name,end_station_name,start_station_code,end_station_code,start_date";
        for(String stringTmp:stringType.split(splitSeq)){
            inputFields.add(DataTypes.createStructField(stringTmp,DataTypes.StringType,true));
        }
        return DataTypes.createStructType(inputFields);
    }

    public static void main(String[] args) throws IOException {
        TrainCrawlSpider trainCrawlSpider =new TrainCrawlSpider();
        trainCrawlSpider.runAll(args,true,true);
    }

    private  String getTrainStopoversInfo( String no, String fromPlaceCode, String toPlaceCode, String date) {
        CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
        JSONObject map = new JSONObject();
        String partnerid = "tclycom";
        map.put("partnerid", partnerid);
        map.put("method", "get_train_info");
        String reqtime = LocalDateTime.now().format(DateUtils.TIME_FORMAT_YYYYMMDDHHMMSS);
        map.put("reqtime", reqtime);//请求时间，格式：yyyyMMddHHmmss（非空）例：20140101093518
        String sign = EncodeUtils.MD5_32Bit(partnerid + "get_train_info" + reqtime + EncodeUtils.MD5_32Bit("LD35*FDS03EX823BEA7UIRV20PI"));
        map.put("sign", sign);
        map.put("train_date", date);
        map.put("from_station", fromPlaceCode);
        map.put("to_station", toPlaceCode);
        map.put("train_code", no);
        String url = "http://172.16.140.82:80/train";
        //调用公共火车票接口，获取班次
        String result = "";
        try {
            result = HttpUtils.doPostTrain(url, JSONObject.toJSONString(map), closeableHttpClient);
        } catch (Exception e) {
            logger.error(
                    "公共班次接口调用失败：" + result + ",参数为：" +
                            JSONObject.toJSONString(map) + "，url为：" + url + ",异常：" + e.getMessage());
        }finally {
            try {
                closeableHttpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
