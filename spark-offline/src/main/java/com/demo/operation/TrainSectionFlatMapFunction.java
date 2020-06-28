package com.demo.operation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.demo.bean.spider.TrainSectionBO;
import com.demo.util.DateUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;

public class TrainSectionFlatMapFunction implements FlatMapFunction<Row, TrainSectionBO> {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Iterator<TrainSectionBO> call(Row row) throws Exception {
        List<TrainSectionBO> trainSectionBOList = new ArrayList<TrainSectionBO>();
        try {
            Map<Integer, String> stationNameMap = new HashMap<Integer, String>();
            Map<Integer, LocalDateTime> arriveTimeMap = new HashMap<Integer, LocalDateTime>();
            Map<Integer, LocalDateTime> leaveTimeMap = new HashMap<Integer, LocalDateTime>();
            Map<Integer,Integer> sequenceMap=new HashMap<Integer, Integer>();
            int size = parseTime(row, stationNameMap, arriveTimeMap, leaveTimeMap,sequenceMap);
            if (size == 0) {
                return trainSectionBOList.iterator();
            }
            fixTimeWithDay(arriveTimeMap, leaveTimeMap, size);
            trainSectionBOList=explodeTrainSection(row, stationNameMap, arriveTimeMap, leaveTimeMap,sequenceMap, size);
        } catch (Exception e) {
            logger.error("row is :" + row + " ; the error reason is:" + e.getMessage(), e);
        }
        return trainSectionBOList.iterator();
    }

    //将date 2019-01-11 time(string类型) 23:20拼接转换为date 2019-01-11 23:20:00
    private int parseTime(Row row, Map<Integer, String> stationNameMap, Map<Integer, LocalDateTime> arriveTimeMap, Map<Integer, LocalDateTime> leaveTimeMap, Map<Integer,Integer> sequenceMap) throws ParseException {
        String date = row.<String>getAs("date").trim();
        String data = row.<String>getAs("data").trim();
        JSONArray array = JSON.parseArray(data);
        int size = array.size();
        if (size == 0) {
            return 0;
        }
        for (int i = 0; i < size && size > 0; i++) {
            JSONObject object = array.getJSONObject(i);
            String stationName = object.getString("station_name").trim();
            String arriveTime = object.getString("arrive_time").trim();
            String leaveTime = object.getString("start_time").trim();
            if (arriveTime.indexOf("-") != -1) {
                arriveTime = leaveTime;
            }
            stationNameMap.put(i + 1, stationName);
            arriveTimeMap.put(i + 1, LocalDateTime.parse(date + arriveTime, DateUtils.TIME_FORMAT_YYYY_MM_DDHHMM));
            leaveTimeMap.put(i + 1, LocalDateTime.parse(date + leaveTime,DateUtils.TIME_FORMAT_YYYY_MM_DDHHMM));
//            Integer sequence=object.getInteger("station_no");
//            sequenceMap.put(i+1,sequence);
            //排序重排
            sequenceMap.put(i+1,i+1);
        }
        return size;
    }
    /**
     * 修正时间，例如
     * 站点sequence:4  leave_time: 2019-01-11 23:40:00 arrive_time:2019-01-11 01:12:00
     * 将arrive_time修正为2019-01-12 01:12:00,后面的5,6,7站点都加1天.
     * 如,修正前:
     * sequence:1,station_name:通辽,arrive_time:2019-03-07 12:28:00,leave_time:2019-03-07 12:28:00
     * sequence:2,station_name:甘旗卡,arrive_time:2019-03-07 13:18:00,leave_time:2019-03-07 13:21:00
     * sequence:3,station_name:彰武,arrive_time:2019-03-07 14:06:00,leave_time:2019-03-07 14:10:00
     * sequence:4,station_name:新立屯,arrive_time:2019-03-07 14:47:00,leave_time:2019-03-07 14:52:00
     * sequence:5,station_name:阜新南,arrive_time:2019-03-07 15:41:00,leave_time:2019-03-07 16:52:00
     * sequence:6,station_name:清河门北,arrive_time:2019-03-07 17:14:00,leave_time:2019-03-07 17:17:00
     * sequence:7,station_name:义县,arrive_time:2019-03-07 17:51:00,leave_time:2019-03-07 17:53:00
     * sequence:8,station_name:锦州,arrive_time:2019-03-07 18:43:00,leave_time:2019-03-07 19:04:00
     * sequence:9,station_name:葫芦岛,arrive_time:2019-03-07 19:37:00,leave_time:2019-03-07 19:39:00
     * sequence:10,station_name:兴城,arrive_time:2019-03-07 19:55:00,leave_time:2019-03-07 19:57:00
     * sequence:11,station_name:绥中,arrive_time:2019-03-07 20:28:00,leave_time:2019-03-07 20:30:00
     * sequence:12,station_name:山海关,arrive_time:2019-03-07 21:17:00,leave_time:2019-03-07 21:23:00
     * sequence:13,station_name:滦县,arrive_time:2019-03-07 22:37:00,leave_time:2019-03-07 22:53:00
     * sequence:14,station_name:唐山,arrive_time:2019-03-07 23:44:00,leave_time:2019-03-07 00:07:00
     * sequence:15,station_name:天津,arrive_time:2019-03-07 01:31:00,leave_time:2019-03-07 01:38:00
     * sequence:16,station_name:沧州,arrive_time:2019-03-07 02:58:00,leave_time:2019-03-07 03:03:00
     * sequence:17,station_name:德州,arrive_time:2019-03-07 04:19:00,leave_time:2019-03-07 04:21:00
     * sequence:18,station_name:济南,arrive_time:2019-03-07 05:50:00,leave_time:2019-03-07 06:01:00
     * sequence:19,station_name:泰山,arrive_time:2019-03-07 06:54:00,leave_time:2019-03-07 06:56:00
     * sequence:20,station_name:兖州,arrive_time:2019-03-07 07:54:00,leave_time:2019-03-07 07:56:00
     * sequence:21,station_name:滕州,arrive_time:2019-03-07 08:34:00,leave_time:2019-03-07 08:38:00
     * sequence:22,station_name:徐州,arrive_time:2019-03-07 09:56:00,leave_time:2019-03-07 10:06:00
     * sequence:23,station_name:宿州,arrive_time:2019-03-07 10:53:00,leave_time:2019-03-07 11:02:00
     * sequence:24,station_name:蚌埠,arrive_time:2019-03-07 12:01:00,leave_time:2019-03-07 12:04:00
     * sequence:25,station_name:滁州北,arrive_time:2019-03-07 13:22:00,leave_time:2019-03-07 13:25:00
     * sequence:26,station_name:南京,arrive_time:2019-03-07 14:09:00,leave_time:2019-03-07 14:17:00
     * sequence:27,station_name:丹阳,arrive_time:2019-03-07 15:16:00,leave_time:2019-03-07 15:19:00
     * sequence:28,station_name:常州,arrive_time:2019-03-07 15:48:00,leave_time:2019-03-07 15:51:00
     * sequence:29,station_name:无锡,arrive_time:2019-03-07 16:18:00,leave_time:2019-03-07 16:22:00
     * sequence:30,station_name:苏州,arrive_time:2019-03-07 16:50:00,leave_time:2019-03-07 16:54:00
     * sequence:31,station_name:上海,arrive_time:2019-03-07 18:15:00,leave_time:2019-03-07 18:15:00
     * 修正后为:
     * sequence:1,station_name:通辽,arrive_time:2019-03-07 12:28:00,leave_time:2019-03-07 12:28:00
     * sequence:2,station_name:甘旗卡,arrive_time:2019-03-07 13:18:00,leave_time:2019-03-07 13:21:00
     * sequence:3,station_name:彰武,arrive_time:2019-03-07 14:06:00,leave_time:2019-03-07 14:10:00
     * sequence:4,station_name:新立屯,arrive_time:2019-03-07 14:47:00,leave_time:2019-03-07 14:52:00
     * sequence:5,station_name:阜新南,arrive_time:2019-03-07 15:41:00,leave_time:2019-03-07 16:52:00
     * sequence:6,station_name:清河门北,arrive_time:2019-03-07 17:14:00,leave_time:2019-03-07 17:17:00
     * sequence:7,station_name:义县,arrive_time:2019-03-07 17:51:00,leave_time:2019-03-07 17:53:00
     * sequence:8,station_name:锦州,arrive_time:2019-03-07 18:43:00,leave_time:2019-03-07 19:04:00
     * sequence:9,station_name:葫芦岛,arrive_time:2019-03-07 19:37:00,leave_time:2019-03-07 19:39:00
     * sequence:10,station_name:兴城,arrive_time:2019-03-07 19:55:00,leave_time:2019-03-07 19:57:00
     * sequence:11,station_name:绥中,arrive_time:2019-03-07 20:28:00,leave_time:2019-03-07 20:30:00
     * sequence:12,station_name:山海关,arrive_time:2019-03-07 21:17:00,leave_time:2019-03-07 21:23:00
     * sequence:13,station_name:滦县,arrive_time:2019-03-07 22:37:00,leave_time:2019-03-07 22:53:00
     * sequence:14,station_name:唐山,arrive_time:2019-03-07 23:44:00,leave_time:2019-03-08 00:07:00
     * sequence:15,station_name:天津,arrive_time:2019-03-08 01:31:00,leave_time:2019-03-08 01:38:00
     * sequence:16,station_name:沧州,arrive_time:2019-03-08 02:58:00,leave_time:2019-03-08 03:03:00
     * sequence:17,station_name:德州,arrive_time:2019-03-08 04:19:00,leave_time:2019-03-08 04:21:00
     * sequence:18,station_name:济南,arrive_time:2019-03-08 05:50:00,leave_time:2019-03-08 06:01:00
     * sequence:19,station_name:泰山,arrive_time:2019-03-08 06:54:00,leave_time:2019-03-08 06:56:00
     * sequence:20,station_name:兖州,arrive_time:2019-03-08 07:54:00,leave_time:2019-03-08 07:56:00
     * sequence:21,station_name:滕州,arrive_time:2019-03-08 08:34:00,leave_time:2019-03-08 08:38:00
     * sequence:22,station_name:徐州,arrive_time:2019-03-08 09:56:00,leave_time:2019-03-08 10:06:00
     * sequence:23,station_name:宿州,arrive_time:2019-03-08 10:53:00,leave_time:2019-03-08 11:02:00
     * sequence:24,station_name:蚌埠,arrive_time:2019-03-08 12:01:00,leave_time:2019-03-08 12:04:00
     * sequence:25,station_name:滁州北,arrive_time:2019-03-08 13:22:00,leave_time:2019-03-08 13:25:00
     * sequence:26,station_name:南京,arrive_time:2019-03-08 14:09:00,leave_time:2019-03-08 14:17:00
     * sequence:27,station_name:丹阳,arrive_time:2019-03-08 15:16:00,leave_time:2019-03-08 15:19:00
     * sequence:28,station_name:常州,arrive_time:2019-03-08 15:48:00,leave_time:2019-03-08 15:51:00
     * sequence:29,station_name:无锡,arrive_time:2019-03-08 16:18:00,leave_time:2019-03-08 16:22:00
     * sequence:30,station_name:苏州,arrive_time:2019-03-08 16:50:00,leave_time:2019-03-08 16:54:00
     * sequence:31,station_name:上海,arrive_time:2019-03-08 18:15:00,leave_time:2019-03-08 18:15:00
     */
    private void fixTimeWithDay(Map<Integer, LocalDateTime> arriveTimeMap, Map<Integer, LocalDateTime> leaveTimeMap, int size) {
        int n = 0;
        int i = 1;
        int j = 1;
        int day = 0;
        while (n < size * 2) {
            if (n % 2 == 0) {
                LocalDateTime arriveDate = arriveTimeMap.get(i);
                LocalDateTime leaveDate = leaveTimeMap.get(i - 1);
                LocalDateTime arriveDatePlus =arriveDate.plusDays(day);
                arriveTimeMap.put(i, arriveDatePlus);
                if (leaveDate != null && arriveDatePlus.compareTo(leaveDate) < 0) {
                    day += 1;
                    arriveTimeMap.put(i, arriveDatePlus.plusDays(1));
                }
                i++;
            } else {
                LocalDateTime arriveDate = arriveTimeMap.get(j);
                LocalDateTime leaveDate = leaveTimeMap.get(j);
                LocalDateTime leaveDatePlus = leaveDate.plusDays(day);
                leaveTimeMap.put(j, leaveDatePlus);
                if (leaveDatePlus.compareTo(arriveDate) < 0) {
                    day += 1;
                    leaveTimeMap.put(j,leaveDatePlus.plusDays(1));
                }
                j++;
            }
            n++;
        }
    }
    /**
     * 迭代拆成班段表
     * 1-2,1-3,....1-31
     * 2-3,2-4.....2-31
     * ....
     * ..
     * 30-31
     */
    private List<TrainSectionBO> explodeTrainSection(Row row, Map<Integer, String> stationNameMap, Map<Integer, LocalDateTime> arriveTimeMap, Map<Integer, LocalDateTime> leaveTimeMap,Map<Integer,Integer> sequenceMap, int size) {
        List<TrainSectionBO> trainSectionBOList = new ArrayList<TrainSectionBO>();
        String date = row.<String>getAs("date").trim();
        String trainNo = row.<String>getAs("train_no").trim();
        String transportCode = row.<String>getAs("train_code").trim();
        //根据修正后经停表，迭代出火车班段表
        for (int i = 1; i <= size - 1; i++) {
            for (int j = i + 1; j <= size; j++) {
                TrainSectionBO trainSectionBO = TrainSectionBO.builder().transportCode(transportCode).trainNo(trainNo).durationDate(date).beginId(sequenceMap.get(i)).beginStationName(stationNameMap.get(i)).
                        beginTime(leaveTimeMap.get(i).format(DateUtils.TIME_FORMAT_YYYY_MM_DD_HHMMSS)).endId(sequenceMap.get(j)).endStationName(stationNameMap.get(j)).endTime(arriveTimeMap.get(j).format(DateUtils.TIME_FORMAT_YYYY_MM_DD_HHMMSS)).build();
                trainSectionBOList.add(trainSectionBO);
            }
        }
        return trainSectionBOList;
    }
}
