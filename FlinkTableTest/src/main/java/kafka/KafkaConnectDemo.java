package kafka;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
/**
 * @author ZhangPeng
 * @ProjectName UserBehaviorAnalysis
 * @PackageName kafka
 * @Email ZhangPeng1853093@126.com
 * @date 2020/3/26 - 9:42
 */
public class KafkaConnectDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(env);
        sTableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .startFromLatest()
                .property("group.id", "g1")
                .property("bootstrap.servers", "Spark:9092")
        ).withFormat(
                new Json()
                        .failOnMissingField(true)
                        .deriveSchema()
        ).withSchema(
                new Schema()
                        .field("userId", Types.LONG()) //一层嵌套json
                        .field("day", Types.STRING())
                        .field("data", ObjectArrayTypeInfo.getInfoFor(
                                Row[].class,
                                Types.ROW(
                                        new String[]{"package", "activetime"},
                                        new TypeInformation[] {Types.STRING(), Types.LONG()}
                                )
                        ))
        ).inAppendMode().registerTableSource("userlog");
        Table table = sTableEnv.sqlQuery("select * from userlog");
        sTableEnv.toRetractStream(table,Row.class).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
