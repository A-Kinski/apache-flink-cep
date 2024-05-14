package ru.beeline.dgt.maven;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Pair<Integer, String>> inputStream = env.fromData(List.of(
                //ожидаемый результат - abcde
                Pair.of(1,"a"), Pair.of(1,"b"), Pair.of(1, "c"),
                Pair.of(1, "d"), Pair.of(1, "e"),

                //ожидаемый результат - abbcde
                Pair.of(2, "a"), Pair.of(2, "b"), Pair.of(2, "b"),
                Pair.of(2, "c"), Pair.of(2, "d"), Pair.of(2, "e"),

                //неверный порядок, ожидаемый результат - ничего
                Pair.of(3, "a"), Pair.of(3, "b"), Pair.of(3, "d"),
                Pair.of(3, "c"), Pair.of(3, "e"),

                //Нет начального события
                Pair.of(4, "b"), Pair.of(4, "c"),
                Pair.of(4, "d"), Pair.of(4, "e")
        ));

        Pattern<Pair<Integer, String>, ?> cepPattern = Pattern.<Pair<Integer, String>>begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(SimpleCondition.of(value -> "a".equals(value.getValue())))
                .next("b").where(SimpleCondition.of(value -> "b".equals(value.getValue()))).oneOrMore()
                .next("c").where(SimpleCondition.of(value -> "c".equals(value.getValue())))
                .next("d").where(SimpleCondition.of(value -> "d".equals(value.getValue())))
                .next("e").where(SimpleCondition.of(value -> "e".equals(value.getValue())));

        PatternStream<Pair<Integer, String>> patternStream = CEP
                .pattern(inputStream.keyBy(pair -> pair.getKey()), cepPattern).inProcessingTime();

        SingleOutputStreamOperator<String> result = patternStream.process(new PatternProcessFunction<Pair<Integer, String>, String>() {
            @Override
            public void processMatch(Map<String, List<Pair<Integer, String>>> match, Context ctx, Collector<String> out)
                    throws Exception {
                String outString = match.values().stream()
                        .flatMap(Collection::stream).map(Pair::getValue).collect(Collectors.joining())
                        + match.get("a").get(0).getKey();
                out.collect(outString);
            }
        });

        result.print();

        env.execute("CEPTest");
    }
}