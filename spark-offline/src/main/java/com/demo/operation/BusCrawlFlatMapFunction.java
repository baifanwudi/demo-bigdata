package com.demo.operation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.demo.bean.spider.BusCrawlData;
import com.demo.bean.spider.BusRequest;
import com.demo.util.DateUtils;
import com.demo.util.EncodeUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BusCrawlFlatMapFunction implements FlatMapFunction<Row, BusCrawlData> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    String appAccount = "bus.intelligence";

    String appKey = "31E3E0D213AD156BDED7499FBD2AB68F";

    Integer queryTimeOut = 4;

    String newBusNosUrl = "http://tcmobileapi.17usoft.com/busapi3/scheduleApi/getScheduleList";

    @Override
    public Iterator<BusCrawlData> call(Row row) throws Exception {
        Integer startCityId = row.<Integer>getAs("start_city_id");
        Integer startBusCityId=row.<Integer>getAs("start_bus_city_id");
        String startBusName = row.<String>getAs("start_bus_name").trim();
        Integer endCityId = row.<Integer>getAs("end_city_id");
        Integer endBusCityId=row.<Integer>getAs("end_bus_city_id");
        String endBusName = row.<String>getAs("end_bus_name").trim();
        CloseableHttpClient closeableHttpClient = HttpClientBuilder.create().build();
        List<BusCrawlData> busCrawlDataList = new ArrayList<>();
        try {
            for (int i = 0; i < 7; i++) {
                LocalDate now = LocalDate.now();
                String durationDate = now.plusDays(i).toString();
                BusCrawlData busCrawlData = getParseResult(closeableHttpClient, startCityId, startBusCityId,startBusName, endCityId, endBusCityId,endBusName, durationDate);
                busCrawlDataList.add(busCrawlData);
            }
        } catch (Exception e) {
            logger.error("查询" + row.toString() + "失败，操作异常" + e.getMessage(), e);
        } finally {
            try {
                closeableHttpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return busCrawlDataList.iterator();
    }

    private BusCrawlData getParseResult(CloseableHttpClient closeableHttpClient, Integer startCityId, Integer startBusCityId,String startBusName, Integer endCityId, Integer endBusCityId,String endBusName, String durationDate) {
        String key = String.format("%s@%s@%s@%s@%s@%s@%s", startCityId, endCityId, startBusName, endBusName, durationDate,startBusCityId,endBusCityId);
        BusCrawlData crawlData = new BusCrawlData();
        String createTime = LocalDateTime.now().format(DateUtils.TIME_FORMAT_YYYY_MM_DD_HHMMSS);
        crawlData.setDate(durationDate);
        crawlData.setCreateTime(createTime);
        crawlData.setKey(key);
        try {
            String searchResult = getBusSectionLineInfo(closeableHttpClient,startBusCityId, startBusName,endBusCityId, endBusName, durationDate);
            JSONObject jsonObject = JSONObject.parseObject(searchResult);
            JSONObject header = jsonObject.getJSONObject("header");
            Boolean isSuccess = header.getBoolean("isSuccess");
            if (isSuccess) {
                JSONObject body = jsonObject.getJSONObject("body");
                JSONArray jsonArray = body.getJSONArray("schedule");
                crawlData.setStatus("success");
                crawlData.setData(jsonArray.toJSONString());
            } else {
                crawlData.setStatus("fail");
                crawlData.setData("查询" + key + "失败,无返回结果");
            }
        } catch (Exception e) {
            crawlData.setStatus("system-error");
            crawlData.setData(e.getMessage());
            logger.error("查询" + key + "失败，系统操作异常" + e.getMessage(), e);
        }
        return crawlData;
    }

    private String getBusSectionLineInfo(CloseableHttpClient closeableHttpClient,Integer startBusCityId , String startBusName,Integer endBusCityId, String endBusName, String durationDate) {
        BusRequest busRequest = new BusRequest(startBusCityId,startBusName, endBusCityId,endBusName, durationDate, appAccount, appKey);
        String signVal = JSON.toJSONString(busRequest, SerializerFeature.MapSortField, SerializerFeature.SkipTransientField,
                SerializerFeature.WriteNonStringKeyAsString, SerializerFeature.WriteNonStringValueAsString);
        String sign = EncodeUtils.md5(EncodeUtils.md5(signVal) + appKey);
        String finalUrl = newBusNosUrl + "?sign=" + sign;
        return postRequest(closeableHttpClient, finalUrl, signVal);
    }

    private String postRequest(CloseableHttpClient closeableHttpClient, String url, String data) {
        String content = "";
        HttpPost httpPost = new HttpPost(url);
        StringEntity postingString = new StringEntity(data, StandardCharsets.UTF_8);
        postingString.setContentType("application/json;charset=utf-8");
        httpPost.setEntity(postingString);
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(2 * 1000)
                .setConnectTimeout(2 * 1000).setSocketTimeout(queryTimeOut * 1000).build();
        httpPost.setConfig(requestConfig);
        try {
            HttpResponse response = closeableHttpClient.execute(httpPost);
            content = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {

        }
        return content;
    }
}
