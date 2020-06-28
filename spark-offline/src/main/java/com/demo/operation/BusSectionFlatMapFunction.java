package com.demo.operation;

import com.alibaba.fastjson.JSON;
import com.demo.bean.spider.BusLineBO;
import com.demo.bean.spider.BusSectionBO;
import com.demo.util.DateUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BusSectionFlatMapFunction implements FlatMapFunction<Row, BusSectionBO> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Iterator<BusSectionBO> call(Row row) throws Exception {
            try {
                String key = row.<String>getAs("key").trim();
                String[] arrays = key.split("@");
                Integer starCityId = Integer.parseInt(arrays[0]);
                Integer endCityId = Integer.parseInt(arrays[1]);
                String startBusName = arrays[2].trim();
                String endBusName = arrays[3].trim();
                String durationDate = row.<String>getAs("date");
                String lines = row.<String>getAs("data");
                List<BusSectionBO> busSectionBOList = new ArrayList<BusSectionBO>();
                List<BusLineBO> boList = JSON.parseArray(lines, BusLineBO.class);
                for (BusLineBO busLineBO : boList) {
                    Long runTime=busLineBO.getRunTime().equals("")?0L:new Double(Double.parseDouble(busLineBO.getRunTime())*3600).longValue();
                    Double distance=parseDistance(busLineBO);
                    LocalDateTime startDateTime= LocalDateTime.parse(durationDate + busLineBO.getDptTime(),DateUtils.TIME_FORMAT_YYYY_MM_DDHHMM);
                    LocalDateTime endDateTime= startDateTime.plusSeconds(runTime);
                    String startTime= startDateTime.format(DateUtils.TIME_FORMAT_YYYY_MM_DD_HHMMSS);
                    String endTime=endDateTime.format(DateUtils.TIME_FORMAT_YYYY_MM_DD_HHMMSS);
                    BusSectionBO busSectionBO = BusSectionBO.builder().busNo(busLineBO.getScheduleNo()).durationDate(durationDate)
                            .startCityId(starCityId).startBusCityId(busLineBO.getDepId()).startBusCityName(startBusName).startStationName(busLineBO.getDptStation())
                            .endCityId(endCityId).endBusCityId(busLineBO.getDesId()).endBusCityName(endBusName).endStationName(busLineBO.getArrStation())
                            .startTime(startTime).endTime(endTime).runTime(runTime).ticketPrice(busLineBO.getTicketPrice())
                            .distance(distance)
                            .build();
                    busSectionBOList.add(busSectionBO);
                }

                return busSectionBOList.iterator();
            }catch (Exception e){
                logger.error("查询" + row.toString() + "失败，操作异常" + e.getMessage(), e);
            }
            return new ArrayList<BusSectionBO>().iterator();
    }


    private Double parseDistance(BusLineBO busLineBO){
        String distance=busLineBO.getDistance().trim();
        if(distance.equals("") || distance.equals("null")){
            return 0.0;
        }else {
           return Double.parseDouble(distance)*1000;
        }
    }
}
