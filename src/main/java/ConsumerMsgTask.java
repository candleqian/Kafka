import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerMsgTask implements Runnable {
    private static Logger logger = LogManager.getLogger(ConsumerMsgTask.class);
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerMsgTask(KafkaStream stream, int threadNumber) {
        m_threadNumber = threadNumber;
        m_stream = stream;
    }

    public void run() {


        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
            logger.info("Thread " + m_threadNumber + ": "
                    + new String(it.next().message()));
        logger.info("Shutting down Thread: " + m_threadNumber);
    }
}