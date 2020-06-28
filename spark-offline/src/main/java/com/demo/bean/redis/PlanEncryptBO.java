package com.demo.bean.redis;

import lombok.Data;

@Data
public class PlanEncryptBO {

    private String planKey;

    private Long totalNum;

    private String transferCity;

    private String transferType;

    private Long createTime;

    private Integer totalTime;

    private Double totalPrice;
}
