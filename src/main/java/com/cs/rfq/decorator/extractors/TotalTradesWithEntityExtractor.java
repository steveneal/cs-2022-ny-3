package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalTradesWithEntityExtractor implements RfqMetadataExtractor {

    private DateTime today;

    public TotalTradesWithEntityExtractor() {
        this.today = DateTime.now();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        //DateTime today = new DateTime("2019-06-07");
        long todayMs = today.withMillisOfDay(0).getMillis();
        long pastWeekMs = today.withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = today.withMillis(todayMs).minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long tradesToday = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(todayMs))).count();
        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs))).count();
        long tradesPastYear = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastYearMs))).count();

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradesWithEntityToday, tradesToday);
        results.put(tradesWithEntityPastWeek, tradesPastWeek);
        results.put(tradesWithEntityPastYear, tradesPastYear);
        return results;
    }

    @Override
    public void setSince(String since) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime dt = formatter.parseDateTime(since);
        this.today = dt;
    }

}
