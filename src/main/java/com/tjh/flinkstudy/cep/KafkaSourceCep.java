package com.tjh.flinkstudy.cep;

import com.tjh.flinkstudy.util.ConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Map;

/**
 * Created by tjh.
 * Date: 2019/5/6 下午6:55
 **/
public class KafkaSourceCep {
    public static void main( String[] args )
    {
        try {
            System.out.println("进入main函数");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            ////加载kafka配置
            Map properties = ConfigUtil.getAllMessage("kafkaconsumer");
            ParameterTool parameterTool = ParameterTool.fromMap(properties);
            FlinkKafkaConsumer010 consumer010 = new FlinkKafkaConsumer010(
                    parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());

            DataStream<String> messageStream = env
                    .addSource(consumer010).assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
            ;
            System.out.println("订阅成功");
            DataStream<String> temp = messageStream.map(new MapFunction<String, String>() {
                @Override
                public String map(String value) throws Exception {
                    ////这里可以对数据进行清洗，然后返回对应的对象，这里方便演示，不做数据清洗
                    return value;
                }
            });

            Pattern<String, String> loginFailPattern = Pattern.<String>
                    begin("begin")
                    .where(new IterativeCondition<String>() {
                        @Override
                        public boolean filter(String loginEvent, Context context) throws Exception {
                            return loginEvent.contains("apply_risk");
                        }
                    })
                    .next("next")
                    .where(new IterativeCondition<String>() {
                        @Override
                        public boolean filter(String loginEvent, Context context) throws Exception {
                            return loginEvent.contains("apply_risk_dp");
                        }
                    });

            PatternStream<String> patternStream = CEP.pattern(
                    temp,
                    loginFailPattern);


            DataStream<String> loginFailDataStream = patternStream.select((Map<String, List<String>> pattern) -> {
                List<String> first = pattern.get("begin");
                List<String> second = pattern.get("next");
                return first.get(0) + "|" + second.get(0);
            });

            loginFailDataStream.print();
            env.execute("CanalConsumer");
        } catch (Exception ex) {
            System.out.println("发生异常：" + ex.getMessage());
        } finally {
            ////是否资源

        }
    }
}
