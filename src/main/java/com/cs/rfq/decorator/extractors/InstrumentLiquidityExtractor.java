package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityExtractor implements RfqMetadataExtractor {

    private DateTime today;

    public InstrumentLiquidityExtractor() {
        this.today = DateTime.now();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        DateTime pastMonth = today.minusMonths(1);

        String query = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastMonth);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> sqlQueryResults = session.sql(query);

        Object liquidityForPastMonth = sqlQueryResults.first().get(0);
        if (liquidityForPastMonth == null) {
            liquidityForPastMonth = 0L;
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.liquidityForPastMonth, liquidityForPastMonth);
        return results;
    }

    @Override
    public void setSince(String since) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        this.today = formatter.parseDateTime(since);
    }
}
