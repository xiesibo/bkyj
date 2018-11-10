package com.snp.bd.bkyj.job;

import com.snp.bd.bkyj.model.TaskCondition;
import com.snp.bd.bkyj.util.Config;
import com.snp.bd.bkyj.util.SubmitTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by x on 2018/8/3.
 */
public class ScanJob {
    static BlockingQueue<Runnable> blockingQueue=new ArrayBlockingQueue<Runnable>(10);
    static ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(10, 20, 1, TimeUnit.MINUTES, blockingQueue);
    static String[] types={"","","Pzfx","Gxfx","Dtjcx"};
    private static Logger logger = LoggerFactory.getLogger(ScanJob.class);
    public static void main(String[] args) {
        System.out.println("args = [" + "start" + "]");
        while (true){
            List<TaskCondition> tasks= JobService.getTask();
            for (TaskCondition task:
                    tasks) {
                String cmd= Config.SPARK_SUBMIT+" --class com.snp.bd.bkyj.main."+types[task.getTYPE()]+" --name "+task.getID()+" --master yarn-cluster --jars /opt/hadoopclient/Spark/spark/lib/streamingClient/kafka-clients-0.8.2.1.jar,/opt/hadoopclient/Spark/spark/lib/streamingClient/kafka_2.10-0.8.2.1.jar,/opt/hadoopclient/Spark/spark/lib/streamingClient/spark-streaming-kafka_2.10-1.5.1.jar  /bkyj.jar "+ URLEncoder.encode(task.getCONDITION());
                SubmitTask st=new SubmitTask(cmd,task.getID());
                threadPoolExecutor.submit(st);
                try {
                    Thread.sleep(1000L);
                }catch (Exception e){
                    logger.info(e.getMessage());
                }
            }
            try {
                Thread.sleep(5000L);
            }catch (Exception e){
                logger.info(e.getMessage());
            }
        }

    }
}
