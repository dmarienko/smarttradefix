/**
 * ----------------------------
 * SmartTrade connector testing
 * (c) 2019, AppliedAlpha
 * http://www.AppliedAlpha.com/
 */
package com.aa.ira.broker.smarttrade.fix;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by IntelliJ IDEA.
 * Date: 1/11/2019
 * Time: 7:59 PM
 */
public class IDGenerator {
    private static final String SECURITY_REQ_ID = "SecurityReqID_";
    private static final String MD_REQ_ID = "MDReqID_";
    private static final String QUOTE_REQ_ID = "QuoteReqID_";
    private AtomicLong reqIdCounter = new AtomicLong(1L);
    private AtomicLong clOrdIdCounter = new AtomicLong(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli());

    public String getQuoteReqId() {
        return QUOTE_REQ_ID + reqIdCounter.getAndIncrement();
    }

    public String getMdReqId() {
        return MD_REQ_ID + reqIdCounter.getAndIncrement();
    }

    public String getSecurityReqId() {
        return SECURITY_REQ_ID + reqIdCounter.getAndIncrement();
    }

    public String getClOrdId() {
        return Long.toString(clOrdIdCounter.getAndIncrement());
    }
}
