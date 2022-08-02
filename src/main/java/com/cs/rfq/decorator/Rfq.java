package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) {
        String rfqInfo = json;
        //convert json literal map to java map
        Type t = new TypeToken<Map<String, String>>() {}.getType();
        Map<String, String> fields = new Gson().fromJson(rfqInfo, t);
        Rfq newrfq = new Rfq();
        newrfq.setId(fields.get("id"));
        newrfq.setIsin(fields.get("instrumentId"));
        newrfq.setTraderId(Long.parseLong(fields.get("traderId")));
        newrfq.setEntityId(Long.parseLong(fields.get("entityId")));
        newrfq.setQuantity(Long.parseLong(fields.get("qty")));
        newrfq.setPrice(Double.parseDouble(fields.get("price")));
        newrfq.setSide(fields.get("side"));
        return newrfq;
    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
