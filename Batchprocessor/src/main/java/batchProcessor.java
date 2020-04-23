import redis.clients.jedis.Jedis;
import java.time.Instant;
import java.sql.*;

public class batchProcessor {
    static Jedis jedis;
    static Connection conn;
    static final int offset = 5;
    static final int numberOfThreads = 2;
    public static void main(String[] args) throws InterruptedException {
        conn = null;
        jedis = new Jedis("localhost");
        System.out.println("Connection to server successfully");
        try  {
            conn = mysqlconnector.getConnection();
            System.out.println(String.format("Connected to database %s "
                    + "successfully.", conn.getCatalog()));
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        String lastProcessedTime = jedis.get("lastProcessedTime");
        if(lastProcessedTime == null)
        {
            jedis.set("lastProcessedTime", String.valueOf(Instant.now().getEpochSecond()));
        }
        System.out.println("lastProcessedTime:"+jedis.get("lastProcessedTime"));
        for(int i=1;i<=numberOfThreads;i++){
            batchProcessorThread temp = new batchProcessorThread();
            temp.start();
            Thread.sleep(1000);
        }
    }
}
