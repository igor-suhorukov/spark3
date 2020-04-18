package com.github.igorsuhorukov.spark;

import io.questdb.ServerMain;
import lombok.SneakyThrows;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkSessionTest {
    @Test
    @SneakyThrows
    void initSession() {
        Path tempDirWithPrefix = Files.createTempDirectory("quest");
        ServerMain.main(new String[]{"-d", tempDirWithPrefix.toString()});
        Connection connection1 = DriverManager.getConnection("jdbc:postgresql://localhost:8812/", "admin", "quest");
        Statement statement = connection1.createStatement();
        int update = statement.executeUpdate("CREATE TABLE t1 (ID long, NAME string , partition_name long , sub_part long )");
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test;INIT=runscript from 'classpath:/init.sql'")){
            try (SparkSession session = SparkSession.builder().master("local[1]").appName("parquet-dump").getOrCreate()){
                JavaSparkContext context = new JavaSparkContext(session.sparkContext());
                Dataset<Row> tbl1 = session.read().format("jdbc").
                        option("url", "jdbc:h2:mem:test").
                        option("useUnicode", "true").
                        option("continueBatchOnError", "true").
                        option("useSSL", "false").
                        option("user", "").
                        option("password", "").
                        option("dbtable", "(select id, name, id%10 as partition_name, id/100000 as sub_part from t1)").
                        load();
                tbl1.show();

                Dataset<Row> partitions = tbl1.repartition(new Column("partition_name"), new Column("sub_part"));

                try {
                    partitions.write().mode(SaveMode.Overwrite)
                            .option("truncate", true).format("jdbc")
                            .option("url", "jdbc:postgresql://localhost:8812/")
                            .option("dbtable", "t1")
                            .option("isolationLevel", "NONE")
                            .option("user", "admin")
                            .option("password", "quest")
                            .save();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                long h2Count = partitions.count();
                System.out.println("TOTAL H2"+ h2Count);
                ResultSet resultSet = statement.executeQuery("select count(*) from t1");
                resultSet.next();
                long qdbCount = resultSet.getLong(1);
                System.out.println("TOTAL QDB"+ qdbCount);

                assertThat(qdbCount).isEqualTo(h2Count);
            }
        }
    }
}
