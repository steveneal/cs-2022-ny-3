package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        String filePath = "src\\test\\resources\\trades\\trades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
//        trades.printSchema();
//        trades.show();

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());
        extractors.add(new AverageTradedPriceExtractor());
        extractors.add(new TradeSideBiasExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
    }

    public void startSocketListener(JavaStreamingContext jssc) throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 9000);
        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        JavaDStream<Rfq> rfqObject = lines.map(line -> Rfq.fromJson(line));
        rfqObject.foreachRDD(rdd -> {
            rdd.collect().forEach(rfq -> processRfq(rfq));
        });
        //TODO: start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();
        extractors.get(0).setSince("2019-06-07");
        extractors.get(1).setSince("2019-06-07");
        extractors.get(2).setSince("2019-06-07");
        extractors.get(3).setSince("2019-06-07");
        extractors.get(4).setSince("2019-06-07");


        //TODO: get metadata from each of the extractors
        metadata.putAll(extractors.get(0).extractMetaData(rfq, this.session, this.trades));
        metadata.putAll(extractors.get(1).extractMetaData(rfq, this.session, this.trades));
        metadata.putAll(extractors.get(2).extractMetaData(rfq, this.session, this.trades));
        metadata.putAll(extractors.get(3).extractMetaData(rfq, this.session, this.trades));
        metadata.putAll(extractors.get(4).extractMetaData(rfq, this.session, this.trades));

        //TODO: publish the metadata
        //System.out.println(metadata);
        for (RfqMetadataFieldNames name: metadata.keySet()) {
            String key = name.toString();
            String value = metadata.get(name).toString();
            System.out.println(key + " " + value);
        }
    }
}
