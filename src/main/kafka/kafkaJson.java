import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

//解析kafka-json格式
public class kafkaJson {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts DOUBLE " +
                ") WITH (" +
                " 'connector' = 'kafka', " +
                " 'topic' = 'sensor', "+
                " 'properties.bootstrap.servers' = '192.168.163.133:9092', " +
                " 'properties.group.id' = 'testGroup', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'format' = 'json', " +
                " 'json.fail-on-missing-field' = 'false', " +
                " 'json.ignore-parse-errors' = 'true' " +
                ")";
//        tableEnv.executeSql(createDDL);
        tableEnv.sqlUpdate(createDDL);
        Table table = tableEnv.sqlQuery("select user_name,url,ts from clickTable");
        tableEnv.createTemporaryView("temp_table",table);
        Table table1 = tableEnv.sqlQuery("select * from temp_table");

        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts DOUBLE " +
                ") WITH (" +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://localhost:3306/test', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'table-name' = 'users' " +
                ")";
        tableEnv.executeSql(createPrintOutDDL);
        table1.executeInsert("printOutTable").print();

        env.execute();
    }
}
