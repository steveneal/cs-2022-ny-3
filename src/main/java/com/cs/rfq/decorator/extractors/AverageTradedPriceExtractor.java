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

public class AverageTradedPriceExtractor implements RfqMetadataExtractor {

    private DateTime today;

    public AverageTradedPriceExtractor() {
        this.today = DateTime.now();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        DateTime pastWeek = today.minusWeeks(1);

        String query = String.format("SELECT avg(LastPX) from trade where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeek);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object avgTrade = sqlQueryResults.first().get(0);
        if (avgTrade == null) {
            avgTrade = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.averageTradedPricePastWeek, avgTrade);
        return results;
    }

    @Override
    public void setSince(String since) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        DateTime dt = formatter.parseDateTime(since);
        this.today = dt;
    }
}
