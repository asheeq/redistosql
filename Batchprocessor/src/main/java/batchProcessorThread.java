import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class batchProcessorThread extends Thread{
    synchronized public void setTime(Long t)
    {
        if (t>Long.valueOf(batchProcessor.jedis.get("lastProcessedTime"))) {
            batchProcessor.jedis.set("lastProcessedTime",String.valueOf(t));
        }
    }
    public void run()
    {
        ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        Connection finalConn = batchProcessor.conn;
        Runnable task = () -> {
            Long time =Long.valueOf(batchProcessor.jedis.get("lastProcessedTime"));
            while(Instant.now().getEpochSecond() - batchProcessor.offset > time){
                if(batchProcessor.jedis.setnx("processing"+time,"true")>0){
                    List<String> s = batchProcessor.jedis.lrange(String.valueOf(time),0,-1);
                    if(!s.isEmpty()) {
                        StringTokenizer str = new StringTokenizer(s.get(0), "|");
                        try {
                            PreparedStatement pstmt = finalConn.prepareStatement(
                                    "INSERT INTO simpletable (TimeStamp,eventID,eventType,eventMessage,eventDate) " +
                                            "VALUES (?,?,?,?,?)");
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
                        batchProcessor.jedis.del(String.valueOf(time));
                    }
                    setTime(time);
                    break;
                }
                else{
                    time+=1;
                }

            }
            System.out.println("lastProcessedTime:"+batchProcessor.jedis.get("lastProcessedTime"));
            System.out.println("currentEpochTime: " + Instant.now().getEpochSecond());
        };
        ses.scheduleAtFixedRate(task, 5,1, TimeUnit.SECONDS);
    }
}
