package cn.spark.study.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    //获取驱动
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    //获取连接，多线程访问并发控制
    public synchronized static Connection getConnection(){
        if(connectionQueue==null){
            connectionQueue = new LinkedList<Connection>();
            for (int i = 0; i <10 ; i++) {
                try {
                    Connection conn = DriverManager.getConnection(
                            "","",""
                            );
                    connectionQueue.push(conn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return connectionQueue.poll();
    }

    //还回去链接
    public static void returnConnection(Connection coon){
        connectionQueue.push(coon);
    }
}