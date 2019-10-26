import java.io.*;

public class Demo {
    public static void main(String[] args) throws IOException {
        FileWriter output = null;
        BufferedWriter writer = null;
        try{
            output = new FileWriter("est.txt");
            writer = new BufferedWriter(output);
            writer.write("dasdasdfgsgsgsfasdf asdfsfds fdsfdsf");
        }finally{
            if(null != writer){
                writer.close();
            }
            if(null != output){
                output.close();
            }
        }

    }
}
