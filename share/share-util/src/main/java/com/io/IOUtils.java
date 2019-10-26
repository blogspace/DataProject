package com.io;

import java.io.*;

public class IOUtils {
    public static void main(String[] args) throws IOException {
        String str = "D:\\admin\\Desktop\\data.txt";
        String dst = "D:\\admin\\Desktop\\datatest.txt";
        copyFileByChars(str, dst);
    }

    public static void readFileByBytes(String str) throws IOException {
        System.out.println("以字节为单位读取文件内容，一次读一个字节：");
        FileInputStream fileInputStream = new FileInputStream(str);
        int tempbyte;
        while ((tempbyte = fileInputStream.read()) != -1) {
            System.out.print(tempbyte);
        }
//        byte[] tempbytes = new byte[100];
//        int byteread = 0;
//        System.out.print(tempbytes, 0, byteread);
        fileInputStream.close();
    }

    public static void readFileByChars(String str) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(str));
        int tempchar;
        while ((tempchar = inputStreamReader.read()) != -1) {
            //屏蔽掉换行符
            if (((char) tempchar) != '\r') {
                System.out.print((char) tempchar);
            }
        }
        inputStreamReader.close();
    }

    public static void writeFileByChars(String str) throws IOException {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(str, true));
        outputStreamWriter.append("hello world");
        outputStreamWriter.close();
    }

    public static void copyFileByChars(String str, String dst) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(str));
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(dst));
        int tempchar;
        while ((tempchar = inputStreamReader.read()) != -1) {
            //屏蔽掉换行符
            if (((char) tempchar) != '\r') {
                outputStreamWriter.append((char) tempchar);
                outputStreamWriter.flush();
            }
        }
        inputStreamReader.close();
        outputStreamWriter.close();

    }

    public static void readFileByLine(String str) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(str));
        String tempString = null;
        while ((tempString = bufferedReader.readLine()) != null) {
            System.out.println(tempString);
        }
        bufferedReader.close();
    }

    public static void copyFile(String str, String dst) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(str));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(dst));
        String tempString = null;
        while ((tempString = bufferedReader.readLine()) != null) {
            bufferedWriter.append(tempString);
            bufferedWriter.newLine();//换行
            bufferedWriter.flush();//需要及时清掉流的缓冲区，万一文件过大就有可能无法写入了
        }
        bufferedReader.close();
        bufferedWriter.close();
        System.out.println("文件复制完成...");
    }

    public static void writeFileByLine(String str) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(str, true));
        bufferedWriter.append("writeFileByLine");
        bufferedWriter.newLine();
        bufferedWriter.flush();
        bufferedWriter.close();
    }

}