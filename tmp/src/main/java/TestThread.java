public class TestThread extends Thread{
    public void run(){
        String name = Thread.currentThread().getName();
        System.out.println("测试"+"执行当前任务的线程为：" + name);
        for(int i=0; i<5; i++){
            System.out.println(name + ":" + i);
        }
    }


}
