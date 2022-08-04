package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest{

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void totalLiquidityWhenTradesMatch() {

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.liquidityForPastMonth);

        assertEquals(1350000L, result);
    }

    @Test
    public void totalLiquidityPriceWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();
        extractor.setSince("2019-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.liquidityForPastMonth);

        assertEquals(0L, result);
    }
}
