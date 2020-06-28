package com.demo.bean.redis;

import lombok.Data;

@Data
public class PlanMinPriceTimeBO {

    private Integer minPriceCityId;

    private Double minTotalPrice;

    private Integer minTimeCityId;

    private Integer minTotalTime;
}
