package demo1.ThreadUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.*;

class MySQL implements Runnable {
    private Connection con = null;
    private static String driver = "com.mysql.jdbc.Driver";
    //private static String url = "jdbc:mysql://localhost:3306/ods?createDatabaseIfNotExist=true&amp;useSSL=false";
    private static String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8";
    private static String username = "root";
    private static String password = "root";
    private static Statement NULL = null;
    private final int taskNum;

    public MySQL(int taskNum) {
        this.taskNum = taskNum;
    }

    public Statement MysqlOpen() {
        try {
            Class.forName(driver); // 加载驱动类
            con = DriverManager.getConnection(url, username, password); // 连接数据库
            if (!con.isClosed()) {
                System.out.println("***数据库成功连接***");
            }
            Statement state = (Statement) con.createStatement();
            return state;
        } catch (ClassNotFoundException e) {
            System.out.println("找不到驱动程序类，加载驱动失败");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("数据库连接失败");
            e.printStackTrace();
        }
        return NULL;
    }

    public void run() {
        readMySQL();
    }

    public void readMySQL() {
        ResultSet sql = null;
        Statement state = MysqlOpen();
        ResultSet tables = null;
        ResultSet counts = null;
        ResultSet columnnames = null;
        ResultSet records = null;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "47.97.47.214:9092,47.97.3.131:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;

        String[] dbs = {"ods"};
        for (String db : dbs) {
            producer = new KafkaProducer<String, String>(properties);
            try {
                tables = state.executeQuery(
                        "select table_name from information_schema.tables where table_schema='" + db + "'");
                List<String> tableList = convertList(tables);
                for (Iterator iterators = tableList.iterator(); iterators.hasNext(); ) {
                    String tablename = String.valueOf(iterators.next()).replace("{TABLE_NAME=", "").replace("}", "");
                    counts = state.executeQuery("select count(*) from " + tablename);
                    List<String> countsList = convertList(counts);
                    int countnum = Integer.valueOf(String.valueOf(countsList.get(0)).replace("{count(*)=", "").replace("}", ""));
                    columnnames = state.executeQuery("select column_name from information_schema.COLUMNS where table_name='" + tablename + "'");
                    List<String> columnnameList = convertList(columnnames);
                    int columnnameSize = columnnameList.size();
                    System.out.println("=========" + tablename + "========");
                    if (countnum <= 1000000) {
                        records = state.executeQuery("select * from " + tablename);// +"'between id" + ((taskNum - 1) *  5000) + " and " + (taskNum *  5000));
                        producer.send(new ProducerRecord<String, String>("mysql_result", StringUtils.join(convertList(records).toArray(), ",")));
                    } else {
                        records = state.executeQuery("select * from " + tablename);
                        producer.send(new ProducerRecord<String, String>("mysql_result", StringUtils.join(convertList(records).toArray(), ",")));
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    tables.close();
                    counts.close();
                    columnnames.close();
                    records.close();
                    state.close();
                    con.close();
                    producer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("---------task " + taskNum + "执行完毕---------");
    }

    public static PreparedStatement prepareStmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    public static ResultSet executeQuery(Statement stmt, String sql) {
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public static void close(Connection conn, Statement stmt, PreparedStatement preStatement, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            conn = null;
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            stmt = null;
        }
        if (preStatement != null) {
            try {
                preStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            preStatement = null;
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            rs = null;
        }
    }

    private static List convertList(ResultSet rs) throws SQLException {
        List list = new ArrayList();
        ResultSetMetaData md = rs.getMetaData();// 获取键名
        int columnCount = md.getColumnCount();// 获取行的数量
        while (rs.next()) {
            Map rowData = new HashMap();// 声明Map
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));// 获取键名及值
            }
            list.add(rowData);
        }
        return list;
    }
}
