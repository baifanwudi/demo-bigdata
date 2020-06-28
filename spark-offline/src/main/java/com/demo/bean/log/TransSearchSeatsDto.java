/**
 * Creation Date:2017年9月22日-上午11:43:39
 * 
 * Copyright 2008-2017 © 同程网 Inc. All Rights Reserved
 */
package com.demo.bean.log;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description Of The Class<br/>
 * QQ:5257075
 * 
 * @author 	Yh(杨恒46555)
 * @version 1.0.0, 2017年9月22日-上午11:43:39
 * @since 2017年9月22日-上午11:43:39
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransSearchSeatsDto {

    /**
     * 座位类型名称
     */
    private String seatName;
    /**
     * 座位类型code
     */
    private Integer seatCode;
    
    /**
     * 座位价格:默认为下铺价格，若无下铺价格则返回上铺价格
     */
    private String price;
    /**
     *
     */
    private String seatState;

    /**
     * 剩余座位数
     */
    private int seats;

    /**
     * 上铺价格
     */
    private String upPrice;

    /**
     * 中铺价格
     */
    private String midPrice;

    /**
     * 下铺价格
     */
    private String downPrice;


    /**
     * 下单订单标识
     */
    private String queryKey;
    /**
     * 上车补票：距离到达站站名
     */
    private String midToStation;
    /**
     * 上车补票：距离到达站三字码
     */
    private String midToStationCode;
    /**
     * 上车补票：距离到达站剩余站数
     * 多买几站：多买站数
     */
    private Integer diffSequence;

    /**
     * 上车补票专用字段，运行时间（分钟）
     */
    private String runTimeSpan;

    /**
     * 到达时间
     */
    private String toTime;
    /**
     * 多买几站：始发站多买站的出发时间
     */
    private String departureStartTime;

    /**
     * 多买几站：到达站站多买站的到达时间
     */
    private String arrivalEndTime;

    /**
     * 多买几站：多花的价格
     *
     */
    private String payMoreMoney;
    /**
     * 上车补票：少花的价格
     */
    private String lessMoney;

    /**
     * 多买几站：未补票站价格
     */
    private int oldPrice;

    /**
     * 多买几站：多买站的站名
     */
    private String maxToStation;
    /**
     * 多买几站：多买站的站三字码
     */
    private String maxToStationCode;

    /**
     * 多买几站：出发3，到达2    说明，出发站往前多买几站标识为3，达到站往后多买标识为2 ,1为补票
     */
    private int flag;
    /**
     * 占座失败更换相似坐席车次号
     */
    private String failureSeatTrainNo;

//    private TrainTransferSeatInfoDto seatInfoDto;

    private boolean seatFlag;

    /**
     * 【网关-按方案推荐坐席】
     * https://www.tapd.cn/20094161/prong/stories/view/1120094161056866718
     * 此功能专用字段,出发时间
     */
    private String fromTime;

}

