package candle.test.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 详细可以参考：https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
 *
 * @author Fung
 */
public class ProducerDemo {
    private static Logger logger = LogManager.getLogger(ProducerDemo.class);
    public static void main(String[] args) {
        Random rnd = new Random();
        int events = 100;

        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list", "node2:9092,node3:9092,node4:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
        props.put("partitioner.class", "candle.test.kafka.SimplePartitioner");
//        props.put("num.partitions", "10");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);

        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // 产生并发送消息
        long start = System.currentTimeMillis();
        for (long i = 0; i < events; i++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + i;//rnd.nextInt(255);
            String msg = "\t" + args[0]+ "\t" + runtime  ;
            //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                    "test2", ip, msg);
            producer.send(data);
            logger.info(ip + msg);
        }
        logger.info("耗时:" + (System.currentTimeMillis() - start));

        // 关闭producer
        producer.close();
    }
}