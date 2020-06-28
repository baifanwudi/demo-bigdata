package com.demo.bean.log;

import lombok.Data;

@Data
public class NearStationLog {
    //出发地，中文名
    private String originFrom;
    //目的地，中文名
    private String originTo;
    //用户搜索时间
    private String date;
    //originFrom名称类型,城市还是站点
    private String fromDataType;
    //originTo名称类型,城市还是站点
    private String toDataType;
    //场景编码
    private String sceneCode;

    private String stationCode;

    private String stationName;

    private String nearStationCode;

    private String nearStationName;
    //距离
    private Integer distance;
    //通行班次数
    private Integer transportNum;
    //最晚到达时间
    private String lastArriveTime;
    //最晚出发时间
    private String lastLeaveTime;
    //中转交通类型,eg:BT
    private String typeCode;
    //请求时间
    private String requestTime;
    //requestId
    private  String requestId;
}
