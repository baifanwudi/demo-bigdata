package com.demo.bean.spider;

import com.demo.util.DateUtils;
import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: chinwer
 * @Date: 2018/9/20 17:24
 * @Description:
 */
@Data
public class BusRequest {

    /**
     * 用户账号信息
     */
    private Map account = new HashMap();

    /**
     * 渠道ID,1-安卓，2-苹果，3-pc，4手q，5-touch，6-WP8，7-微信，8-分销商，9-小程序
     */
    private int plateId;

    /**
     * 出发城市ID，对应公共城市ID，可不传，给个默认值0
     */
    private String depCityId="0";

    /**
     * 出发城市id
     */
    private int depId;

    /**
     * 到达城市ID，对应公共城市ID，可不传，给个默认值0
     */
    private String desCityId="0";

    /**
     * 到达城市id
     */
    private int desId;

    /**
     * 出发城市名称
     */
    private String departure;

    /**
     * 到达城市名称
     */
    private String destination;

    /**
     * 出发时间
     */
    private Date departureDate;

    /**
     * 发车时间,1.凌晨，2.上午，3.中午，4.晚上
     */
    private String dptTimeSpan;

    /**
     * 是否需要筛选信息,默认true，返回带底部车站和发车时段筛选信息
     */
    private boolean hasCategory = true;

    /**
     * 出发车站
     */
    private String departureStation = "";

    /**
     * 到达车站
     */
    private String arrivalStation = "";

    /**
     * 页码
     */
    private int page;

    /**
     * 每页条数
     */
    private int pageSize = 25;

    public BusRequest() {
    }

    public BusRequest(int depId,String departure,int desId, String destination, String departureDate, String appAccount, String appKey) {
        this.depId=depId;
        this.departure = departure;
        this.desId=desId;
        this.destination = destination;
        this.departureDate = DateUtils.parseStringToDate(departureDate);
        this.account.put("appAccount", appAccount);
        //this.account.put("appKey", appKey);
        this.page = 1;
        this.plateId = 7;
        this.pageSize = 9999;
        this.hasCategory = false;
    }
}
