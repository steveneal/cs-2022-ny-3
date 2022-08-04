package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeSideBiasExtractorTest extends AbstractSparkUnitTest{
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
    public void tradeSideBiasBuy() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object weekBuy = meta.get(RfqMetadataFieldNames.tradeSideBuyPastWeek);
        Object monthBuy = meta.get(RfqMetadataFieldNames.tradeSideBuyPastMonth);

        assertEquals(1.0, weekBuy);
        assertEquals(1.0, monthBuy);
    }

    @Test
    public void tradeSideBiasSell() {

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object weekSell = meta.get(RfqMetadataFieldNames.tradeSideSellPastWeek);
        Object monthSell = meta.get(RfqMetadataFieldNames.tradeSideSellPastMonth);

        assertEquals(4.0, weekSell);
        assertEquals(4.0, monthSell);
    }

    @Test
    public void tradeSideBiasNoPreviousBuyOrSell() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ7");

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object weekSell = meta.get(RfqMetadataFieldNames.tradeSideSellPastWeek);
        Object monthBuy = meta.get(RfqMetadataFieldNames.tradeSideBuyPastMonth);

        assertEquals(-1, weekSell);
        assertEquals(-1, monthBuy);
    }




}