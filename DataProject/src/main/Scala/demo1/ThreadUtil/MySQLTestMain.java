package demo1.ThreadUtil;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MySQLTestMain {
    public static void main(String[] args) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(15, 35, 1000, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(25));
        long start = System.currentTimeMillis();
        System.out.println("activeCountMain1 : " + Thread.activeCount());
        for (int i = 1; i <= 20; i++) {
            MySQL mysql = new MySQL(i);
            executor.execute(mysql);
            System.out.println("线程池中线程数目：" + executor.getPoolSize() + "，队列中等待执行的任务数目：" + executor.getQueue().size()
                    + "，已执行玩别的任务数目：" + executor.getCompletedTaskCount());
        }
        executor.shutdown();
        while (true) {
            if (executor.getActiveCount() == 0){
                break;
            }
        }
        System.out.println("activeCountMain2 : " + Thread.activeCount());
        long end = System.currentTimeMillis();
        System.out.println("平均每秒可输出: " + 100000 / (end - start) + " 条");
    }

}