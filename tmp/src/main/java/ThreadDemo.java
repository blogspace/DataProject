
import java.util.Scanner;

public class ThreadDemo {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        //String input = "ready go me";
        String[] str =  sc.nextLine().split(" ");
        StringBuffer stringBuffer2 = new StringBuffer();
        String value = null;
        for (int i = 0; i <str.length ; i++) {
            StringBuffer stringBuffer = new StringBuffer(str[i]);
            value=  stringBuffer.reverse().toString();
            if(i==str.length-1){
                stringBuffer2.append(value);
            }else{
                stringBuffer2.append(value+" ");

            }
        }

        System.out.println(stringBuffer2);
    }
}
