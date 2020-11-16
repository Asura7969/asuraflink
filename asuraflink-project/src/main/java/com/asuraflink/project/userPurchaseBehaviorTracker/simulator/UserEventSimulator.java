package com.asuraflink.project.userPurchaseBehaviorTracker.simulator;

//import com.cloudwise.sdg.dic.DicInitializer;
//import com.cloudwise.sdg.template.TemplateAnalyzer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 数据模拟生成器
 * https://github.com/CloudWise-OpenSource/Data-Processer
 */
public class UserEventSimulator {
    /**
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":196}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12 09:43:18","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":126}}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12 09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
     */
    public static void main(String[] args) throws Exception{
        //加载词典(只需执行一次即可)
        //DicInitializer.init();

        //编辑模版
        String userEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"$Dic{eventType}\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId}}}";

        String purchaseUserEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"PURCHASE\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId},\"price\":$Dic{price},\"amount\":$Dic{amount}}}";

        //创建模版分析器
        //TemplateAnalyzer userEventTplAnalyzer = new TemplateAnalyzer("userEvent", userEventTpl);

        //TemplateAnalyzer purchaseUserEventTplAnalyzer = new TemplateAnalyzer("purchaseUserEventTpl", purchaseUserEventTpl);


        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        ProducerRecord record;
        for(int i = 1; i <= 100000; i ++){
            //分析模版生成模拟数据
            //打印分析结果
            //System.out.println(userEventTplAnalyzer.analyse());
            record = new ProducerRecord<>(
                    "purchasePathAnalysisInPut",
                    null,
                    new Random().nextInt()+"",
                    //userEventTplAnalyzer.analyse());
                    "");
            producer.send(record);
            long sleep = (long) (Math.random()*2000);
            Thread.sleep(sleep);
            System.out.println("------------"+sleep+"----"+sleep%2);
            if(sleep%2==0&&sleep>800){
                System.out.println("------------"+sleep+"----"+sleep%2);
                //System.out.println(purchaseUserEventTplAnalyzer.analyse());
                record = new ProducerRecord<>(
                        "purchasePathAnalysisInPut",
                        null,
                        new Random().nextInt()+"",
                        "");
                        //purchaseUserEventTplAnalyzer.analyse());
                producer.send(record);
            }
        }
    }
}
