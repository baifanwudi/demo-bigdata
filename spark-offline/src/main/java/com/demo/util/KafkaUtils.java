package com.demo.util;

/**
 * Created by allenbai on 2017/2/22 0022.
 */

import com.alibaba.fastjson.JSON;
import com.demo.bean.DataNotification;
import com.demo.common.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class KafkaUtils {

    protected static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    private static KafkaProducer<String, String> producer;

    public final static String TOPIC = ConfigUtil.getPros("kafka.topic");

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", ConfigUtil.getPros("kafka.bootstrap.servers"));
        props.put("acks", "-1");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("client.id","");
        producer = new KafkaProducer<String, String>(props);
    }

    public static void produce(String data) {
        producer.send(new ProducerRecord<String, String>(TOPIC, data));
        producer.close();
    }

    public static void sendFinishNotification(String type) {
        String now = DateUtils.nowDate();
        DataNotification dataNotification = new DataNotification();
        dataNotification.setType(type);
        Map<String, String> content = new HashMap<String, String>();
        content.put("complete", "true");
        content.put("date", now);
        dataNotification.setContent(content);
        String data=JSON.toJSONString(dataNotification);
        KafkaUtils.produce(data);
        logger.info("kafka消息发送成功"+data);
    }
}
