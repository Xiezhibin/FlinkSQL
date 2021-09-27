package testFlink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

public class SQLStreaming {
    public static String brockers = "172.16.0.17:9092";
    public static String inputTopic = "test1";
    public static String baseDir = "hdfs://hadoop3:8082/user/hive/warehouse";
    public static void main(String[] args) throws Exception {

    //set up flink table environment
    // **********************
    // BLINK STREAMING QUERY
    // **********************
    StreamExecutionEnvironment bsEnv = getExecutionEnvironment();

    //set EnvironmentSettings
    bsEnv.setParallelism(1);
    EnvironmentSettings Setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, Setting);

    //Register SOURCE Blink Table + Flink sql connect DDL写法
    //source
    String SQL1 =
          "CREATE TABLE kafkaTable1(\n" +
                "`id` BIGINT,\n" +
                "`site` STRING,\n"+
                "`proctime` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'userbehavior1',\n"+
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'properties.group.id' = 'testGroup',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format'='json')";
    //sink
    String SQL2 =
          "CREATE TABLE kafkaTable2 (\n" +
                "`id` BIGINT,\n" +
                "`site` STRING,\n"+
                "`proctime` TIMESTAMP(3) \n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'userbehavior2',\n"+
                "'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "'properties.group.id' = 'testGroup',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'json')";

    // filter using Flink Table API https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/python/table-api-users-guide/operations.html
    // transformSQL can use some operator to realize some statistics
    String transformSQL =
            "INSERT INTO kafkaTable2 " +
                    "SELECT id,site,proctime " +
                    "FROM kafkaTable1 ";

    tableEnv.executeSql(SQL1);
    tableEnv.executeSql(SQL2);
    tableEnv.executeSql(transformSQL);
    }
}
