package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.aspect.FlinkAspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkAspectTest {

    private static final Logger logger = LoggerFactory.getLogger(FlinkAspect.class);

    public static void main(String[] args) {


        EnvironmentSettings env = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(env);

        String path = "user.csv";
        if (args.length != 0) {
            path = args[0];
        }

        logger.info("read csv file from {}", path);

        String createTableDDL = "CREATE TABLE t_csv (" +
                "    id int," +
                "    name string," +
                "    age int" +
                ") WITH (" +
                "    'connector' = 'filesystem'," +
                "    'path' = '" + path + "'," +
                "    'format' = 'csv'" +
                ")";
        tEnv.executeSql(createTableDDL);
        String sql = "select * from t_csv";
        tEnv.executeSql(sql).print();
    }
}
