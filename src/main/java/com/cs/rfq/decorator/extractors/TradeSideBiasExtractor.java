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

public class TradeSideBiasExtractor implements RfqMetadataExtractor {
    private DateTime today;

    public TradeSideBiasExtractor() {
        this.today = DateTime.now();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        DateTime pastWeek = today.minusWeeks(1);
        DateTime pastMonth = today.minusMonths(1);
        int buy = 1;
        int sell = 2;

        String buyWeek = String.format("SELECT sum(Side) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side='%d'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeek,
                buy);
        String buyMonth = String.format("SELECT sum(Side) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side='%d'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastMonth,
                buy);
        String sellWeek = String.format("SELECT sum(Side) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side='%d'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeek,
                sell);
        String sellMonth = String.format("SELECT sum(Side) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s' AND Side='%d'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastMonth,
                sell);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResultsBuyWeek = session.sql(buyWeek);
        Dataset<Row> sqlQueryResultsBuyMonth = session.sql(buyMonth);
        Dataset<Row> sqlQueryResultsSellWeek = session.sql(sellWeek);
        Dataset<Row> sqlQueryResultsSellMonth = session.sql(sellMonth);


        Object tradeSideBuyWeek = sqlQueryResultsBuyWeek.first().get(0);
        if (tradeSideBuyWeek == null) {
            tradeSideBuyWeek = -1;
        }

        Object tradeSideBuyMonth = sqlQueryResultsBuyMonth.first().get(0);
        if (tradeSideBuyMonth == null) {
            tradeSideBuyMonth = -1;
        }

        Object tradeSideSellWeek = sqlQueryResultsSellWeek.first().get(0);
        if (tradeSideSellWeek == null) {
            tradeSideSellWeek = -1;
        }

        Object tradeSideSellMonth = sqlQueryResultsSellMonth.first().get(0);
        if (tradeSideSellMonth == null) {
            tradeSideSellMonth = -1;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradeSideBuyPastWeek, tradeSideBuyWeek);
        results.put(RfqMetadataFieldNames.tradeSideBuyPastMonth, tradeSideBuyMonth);
        results.put(RfqMetadataFieldNames.tradeSideSellPastWeek, tradeSideSellWeek);
        results.put(RfqMetadataFieldNames.tradeSideSellPastMonth, tradeSideSellMonth);
        return results;
    }
    @Override
    public void setSince(String since) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime dt = formatter.parseDateTime(since);
        this.today = dt;
    }
}
