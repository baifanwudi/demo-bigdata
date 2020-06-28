package com.demo.bean.log;


import lombok.Data;

@Data
public class SeatInfoBO {
    //坐席类型
    private String seatType;
   //坐席余票
    private int ticketNum;
    //座位价格
    private double price;
}
