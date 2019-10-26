import static org.junit.Assert.*;

public class WeTestTest {

    @org.junit.Test
    public void test1() {
        Runnable runnable = new Runnable() {
            public void run() {
                System.out.println("线程启动了");
            }
        };
        runnable.run();
    }
}