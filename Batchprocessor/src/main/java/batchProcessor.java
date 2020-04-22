import redis.clients.jedis.Jedis;
import java.time.Instant;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class batchProcessor {
    public static void main(String[] args) {
        Connection conn = null;
        Jedis jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully");
        try  {
            conn = mysqlconnector.getConnection();
            System.out.println(String.format("Connected to database %s "
                    + "successfully.", conn.getCatalog()));
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        int offset = 5;
        String lastProcessedTime = jedis.get("lastProcessedTime");
        if(lastProcessedTime == null)
        {
            jedis.set("lastProcessedTime", String.valueOf(Instant.now().getEpochSecond()));
        }
        System.out.println("lastProcessedTime:"+jedis.get("lastProcessedTime"));

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(10);

        Connection finalConn = conn;
        Runnable task = () -> {
            System.out.println(Thread.currentThread().getName());
            Long time = Long.valueOf(jedis.get("lastProcessedTime"));
            if(Instant.now().getEpochSecond() - offset > time){
                List<String> s = jedis.lrange(String.valueOf(time),0,-1);
                if(!s.isEmpty()) {
                    StringTokenizer str = new StringTokenizer(s.get(0), "|");
                    Statement st = null;
                    try {
                        PreparedStatement pstmt = finalConn.prepareStatement("INSERT INTO simpletable (TimeStamp,eventID,eventType,eventMessage,eventDate) VALUES (?,?,?,?,?)");
                        pstmt.setString(1, String.valueOf(time));
                        pstmt.setString(2,str.nextToken());
                        pstmt.setString(3,str.nextToken());
                        pstmt.setString(4,str.nextToken());
                        pstmt.setString(5,str.nextToken());
                        pstmt.executeUpdate();
                        System.out.println("Inserted in db");
                    } catch (SQLException sql) {
                        sql.printStackTrace();
                    }
                    jedis.del(String.valueOf(time));
                }
                time += 1;
                jedis.set("lastProcessedTime", String.valueOf(time));
                System.out.println("lastProcessedTime:"+jedis.get("lastProcessedTime"));
            }
            System.out.println("currentEpochTime: " + Instant.now().getEpochSecond());
        };
        ses.scheduleAtFixedRate(task, 0,1, TimeUnit.SECONDS);
    }
}
