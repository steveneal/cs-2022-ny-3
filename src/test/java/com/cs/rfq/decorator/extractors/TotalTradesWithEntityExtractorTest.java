package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalTradesWithEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkTradesWhenAllTradesMatch() {

        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object day = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);
        Object week = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);
        Object year = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(1L, day);
        assertEquals(3L, week);
        assertEquals(5L, year);
    }

    @Test
    public void checkTradesWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalTradesWithEntityExtractor extractor = new TotalTradesWithEntityExtractor();
        extractor.setSince("2017-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedYearToDate);

        assertEquals(null, result);
    }

}