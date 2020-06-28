package com.demo.bean;

import lombok.Data;

/**
 * kafka数据更新完毕通知
 */
@Data
public class DataNotification {

    private String type;

    private Object content;
}
