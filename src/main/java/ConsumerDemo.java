import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by qianchengzhang on 16/2/3.
 */
public class ConsumerDemo {
    private static Logger logger = LogManager.getLogger(ConsumerDemo.class);

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public ConsumerDemo(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        logger.info("main shutdown");
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        logger.info("numThread: {}", numThreads);
        // now launch all the threads
        executor = Executors.newFixedThreadPool(numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        logger.info("streams size is:{}",streams.size());
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerMsgTask(stream, threadNumber));
            threadNumber++;
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper,
                                                       String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] arg) {

        String[] args = {"node3:2181", "group-1", "test", "2"};
        String zooKeeper = args[0];
        String groupId = args[1];//消费者相关,各自消费各自的
        String topic = args[2];
        int threads = Integer.parseInt(arg[0]);

        ConsumerDemo demo = new ConsumerDemo(zooKeeper, groupId, topic);
        demo.run(threads);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {

        }
        demo.shutdown();
    }
}