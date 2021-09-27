/**
 * Skeleton for a Flink Streaming Job.
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 *
 *      http://flink.apache.org/docs/stable/
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
package testFlink;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.*;

/**
 * Contains the example realsed by Flink SQL API.
 * The coding using Kafka connector allows to reading data from and writing data to
 * Kafka topics with exactly-once guarantees.
 */
public class SQLStreaming {

    public static void main(String[] args) throws Exception {

    //set up flink table environment (BLINK STREAMING QUERY）
    StreamExecutionEnvironment bsEnv = getExecutionEnvironment();

    //set up environment including Parallelism and runtime mode
    bsEnv.setParallelism(1);
    EnvironmentSettings Setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, Setting);

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

    //filter using Flink Table API
    String transformSQL =
            "INSERT INTO kafkaTable2 " +
                    "SELECT id,site,proctime " +
                    "FROM kafkaTable1 ";

    //execute query SQL1 SQL2 transformSQL
    tableEnv.executeSql(SQL1);
    tableEnv.executeSql(SQL2);
    tableEnv.executeSql(transformSQL);
    }
}
