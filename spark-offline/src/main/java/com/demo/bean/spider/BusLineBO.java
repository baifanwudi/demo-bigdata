package com.demo.bean.spider;

import lombok.Data;

@Data
public class BusLineBO {
    //距离
    private String distance;
    //行程时间，小时
    private String runTime;
    //出发时间
    private String dptTime;
    //票价
    private Double ticketPrice;
    //出发日期
    private String dptDate;
    //出发站点
    private String dptStation;
    //到达站点
    private String arrStation;
    //出发城市ID
    private Integer depId;
    //出发城市
    private String departure;
    //到达城市ID
    private Integer desId;
    //到达城市
    private String destination;
    //车次号
    private String scheduleNo;
}
