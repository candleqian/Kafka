

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

/**
 * Created by qianchengzhang on 16/1/31.
 */
public class InfluxDBTest {
    private static Logger logger = LogManager.getLogger(InfluxDBTest.class);

    private ExecutorService executorService;

    public InfluxDBTest() {
    }
    public void run(int numberThreads,String server,String local){

        // now launch all the threads
        executorService = Executors.newFixedThreadPool(numberThreads);
        for (int i = 0; i < numberThreads; i++) {
            logger.info("Threads:"+i);
            executorService.submit(new InfluxTask(server,local,Math.random()*200+50));
        }
    }

//    public void init() {
//
//        new MyThread("serverA", "zhejiang", Math.random() * 100L + 20).start();
//        new MyThread("serverA", "shanghai", Math.random() * 100L + 20).start();
//        new MyThread("serverA", "jiangsu", Math.random() * 100L + 20).start();
//        new MyThread("serverB", "shanghai", Math.random() * 100L + 20).start();
//        new MyThread("serverB", "zhejiang", Math.random() * 100L + 20).start();
//        new MyThread("serverB", "jiangsu", Math.random() * 100L + 20).start();
//    }

    public static void main(String[] args) throws Exception {
        InfluxDBTest db = new InfluxDBTest();
        db.run(Integer.parseInt(args[0]),args[1],args[2]);

    }


    public class InfluxTask implements Runnable{

        InfluxDB influxDB = InfluxDBFactory.connect("http://node4:8086", "root", "root");
        String dbName = "enniu";

        final BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .tag("async", "true")
                .retentionPolicy("default")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
        String host, region;
        double tvalue;

        public InfluxTask(String host,String regin,double tvalue) {
            this.host=host;
            this.region =regin;
            this.tvalue=tvalue;
        }

        @Override
        public void run() {

            while (true) {
                Point p1 = Point.measurement("cpu")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .field("host", host)
                        .field("region", region)
                        .field("value", tvalue).build();
                batchPoints.point(p1);
                influxDB.write(batchPoints);
                try {
                    sleep(300L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    public class MyThread extends Thread {
//        InfluxDB influxDB = InfluxDBFactory.connect("http://node4:8086", "root", "root");
//        String dbName = "enniu";
//
//        final BatchPoints batchPoints = BatchPoints
//                .database(dbName)
//                .tag("async", "true")
//                .retentionPolicy("default")
//                .consistency(InfluxDB.ConsistencyLevel.ALL)
//                .build();
//
//        String host, region;
//        double tvalue;
//
//        public MyThread(String host, String region, double tvalue) {
//            this.host = host;
//            this.region = region;
//            this.tvalue = tvalue;
//        }
//
//        public void run() {
//            while (true) {
//                Point p1 = Point.measurement("cpu")
//                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                        .field("host", host)
//                        .field("region", region)
//                        .field("value", tvalue).build();
//                batchPoints.point(p1);
//                influxDB.write(batchPoints);
//                try {
//                    sleep(300L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
}

