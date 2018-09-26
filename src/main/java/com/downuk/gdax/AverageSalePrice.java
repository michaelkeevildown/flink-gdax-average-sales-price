package com.downuk.gdax;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AverageSalePrice {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // setup kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");
        FlinkKafkaConsumer08<ObjectNode> kafkaSource = new FlinkKafkaConsumer08<>(
                "mytopic",
                new JSONKeyValueDeserializationSchema(false),
                properties
        );
        kafkaSource.setStartFromEarliest();

        // consume date from kafka and apply watermarks
        DataStream<ObjectNode> stream = env
                .addSource(kafkaSource)
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

        // take windows stream and mutate, avgerage and write to Elasticsearch
        DataStream<Tuple2<String, Double>> aggregation = stream
                .flatMap(new Extractor())
                // key stream by symbol
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

                // consume keyed window and output the avgerage sales price
                .apply(new WindowFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Double>> values, Collector<Tuple2<String, Double>> out) throws Exception {
                        double sum = 0.0;
                        int count = 0;
                        String symbol = "";
                        for (Tuple2<String, Double> value : values) {
                            sum += value.f1;
                            symbol = value.f0;
                            count++;
                        }

                        double avg = sum / count;

                        double roundOff = Math.round(avg * 1000.0) / 1000.0;

                        out.collect(new Tuple2<String, Double>(symbol, roundOff));
                    }
                });

        // print
        aggregation.print();

        /*
            Elasticsearch Configuration
        */
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple2<String, Double>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Double>>() {
                    private IndexRequest createIndexRequest(Tuple2<String, Double> node) {

                        JSONObject obj = new JSONObject();
                        obj.put("symbol", node.f0);
                        obj.put("price", node.f1);

                        return Requests.indexRequest()
                                .index("avg-price")
                                .type("trade")
                                .source(obj.toString(), XContentType.JSON);
                    }

                    @Override
                    public void process(Tuple2<String, Double> node, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(node));
                    }
                }
        );

        // set number of events to be seen before writing to Elasticsearch
        esSinkBuilder.setBulkFlushMaxActions(1);

        // finally, build and add the sink to the job's pipeline
        aggregation.addSink(esSinkBuilder.build());

        // run job
        env.execute("Avg Sale Price");
    }

    /*
        User Functions
    */

    private static class Extractor implements FlatMapFunction<ObjectNode, Tuple2<String, Double>> {

        @Override
        public void flatMap(ObjectNode trade, Collector<Tuple2<String, Double>> out) {

            JsonNode node = trade.get("value");

            out.collect(new Tuple2<>(
                    node.get("symbol").toString().replace("\"", ""),
                    Double.parseDouble(node.get("lastSalePrice").toString())
            ));
        }

    }


}

