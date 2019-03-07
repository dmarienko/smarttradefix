/**
 * ----------------------------
 * SmartTrade connector testing
 * (c) 2019, AppliedAlpha
 * http://www.AppliedAlpha.com/
 */
package com.aa.ira.broker.smarttrade;

import com.aa.ira.broker.smarttrade.fix.FixLogFactory;
import com.aa.ira.broker.smarttrade.fix.IDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.MemoryStoreFactory;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.MDReqID;
import quickfix.field.MDUpdateType;
import quickfix.field.MarketDepth;
import quickfix.field.NoRelatedSym;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderCapacity;
import quickfix.field.OrderQty;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.SecurityListRequestType;
import quickfix.field.SecurityReqID;
import quickfix.field.Side;
import quickfix.field.SubscriptionRequestType;
import quickfix.field.Symbol;
import quickfix.field.TimeInForce;
import quickfix.field.TradSesReqID;
import quickfix.field.TransactTime;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.MarketDataIncrementalRefresh;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.MarketDataRequestReject;
import quickfix.fix44.MarketDataSnapshotFullRefresh;
import quickfix.fix44.MassQuote;
import quickfix.fix44.MassQuoteAcknowledgement;
import quickfix.fix44.NewOrderSingle;
import quickfix.fix44.Quote;
import quickfix.fix44.QuoteCancel;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.Reject;
import quickfix.fix44.SecurityList;
import quickfix.fix44.SecurityListRequest;
import quickfix.fix44.TradingSessionStatus;
import quickfix.fix44.TradingSessionStatusRequest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class SmartTradeFixTest {
    private Map<String, List<Message>> marketMessages = new LinkedHashMap<>();

    private class App extends quickfix.fix44.MessageCracker implements Application {
        private boolean isOpenToTrade;
        private List<String> availableSymbols = new ArrayList<>();

        @Override
        public void onCreate(SessionID sessionID) {
            log.info(String.format("Created %s", sessionID.toString()));
        }

        @Override
        public void onLogon(SessionID sessionID) {
            isOpenToTrade = true;
            log.info("Logon: " + sessionID.toString());
        }

        @Override
        public void onLogout(SessionID sessionID) {
            isOpenToTrade = false;
            log.info("Get logout callback ...");
        }

        @Override
        public void toAdmin(Message message, SessionID sessionID) {
            try {
                crack(message, sessionID);
            } catch (Exception e) {
                log.error("Error in fromApp <<" + message + ">>", e);
                e.printStackTrace(System.err);
            }
        }

        @Override
        public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
            try {
                crack(message, sessionId);
            } catch (Exception e) {
                log.error("Error in fromAdmin <<" + message + ">>", e);
                e.printStackTrace(System.err);
            }
        }

        @Override
        public void toApp(Message message, SessionID sessionID) throws DoNotSend {
            try {
                crack(message, sessionID);
            } catch (Exception e) {
                log.error("Error in fromApp <<" + message + ">>", e);
                e.printStackTrace(System.err);
            }
        }

        @Override
        public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
            try {
                crack(message, sessionID);
            } catch (Exception e) {
                log.error("Error in fromApp <<" + message + ">>", e);
                e.printStackTrace(System.err);
            }
        }

        void send(Message aMessage, SessionID sessionID) {
            try {
                Session.sendToTarget(aMessage, sessionID);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onMessage(SecurityList message, SessionID sessionId) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            int nSymbols = message.getNoRelatedSym().getValue();
            log.info(String.format("Security list of %d symbols", nSymbols));
            for (Group g : message.getGroups(NoRelatedSym.FIELD)) {
                availableSymbols.add(g.getString(Symbol.FIELD));
            }

            if (message.isSetLastFragment() && message.getLastFragment().getValue()) {
                // TODO: here we need to notify listeners that request was finished
                log.info("Last fragment received !");
            }
        }

        @Override
        public void onMessage(NewOrderSingle message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("New order single " + message);
        }

        @Override
        public void onMessage(Reject message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("Reject " + message.getSessionRejectReason().toString() + " " + message.getText().getValue());
        }

        @Override
        public void onMessage(ExecutionReport message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("ExecutionReport " + message.get(new OrdStatus()));
        }

        @Override
        public void onMessage(MarketDataRequest message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("MarketDataRequest " + message.get(new MDReqID()));
        }

        @Override
        public void onMessage(MarketDataRequestReject message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("MarketDataRequestReject " + message.getText());
        }

        @Override
        public void onMessage(MarketDataSnapshotFullRefresh message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("MarketDataSnapshotFullRefresh " + message.getNoMDEntries().getValue());
            String symbol = message.get(new Symbol()).getValue();
            List<Message> messages = marketMessages.computeIfAbsent(symbol, k -> new ArrayList<>());
            messages.add(message);
        }

        @Override
        public void onMessage(MarketDataIncrementalRefresh message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("MarketDataIncrementalRefresh " + message.getNoMDEntries().getValue());
            String symbol = message.getString(Symbol.FIELD);
            List<Message> messages = marketMessages.computeIfAbsent(symbol, k -> new ArrayList<>());
            messages.add(message);
        }

        @Override
        public void onMessage(QuoteRequest message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("QuoteRequest " + message.get(new QuoteReqID()));
        }

        @Override
        public void onMessage(Quote message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            double bid = message.isSetBidPx() ? message.getBidPx().getValue() : Double.NaN;
            double ask = message.isSetOfferPx() ? message.getOfferPx().getValue() : Double.NaN;
            double bidSize = message.isSetBidSize() ? message.getBidSize().getValue() : Double.NaN;
            double askSize = message.isSetOfferSize() ? message.getOfferSize().getValue() : Double.NaN;
            String sMesg = String.format("[%s] (%s) {%f(%f) | %f(%f)}",
                    message.getSymbol(), message.get(new QuoteID()).toString(),
                    bid, bidSize, ask, askSize);
            log.info(sMesg);
            //            System.out.println(">>> " + sMesg);
        }

        @Override
        public void onMessage(QuoteCancel message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            super.onMessage(message, sessionID);
        }

        @Override
        public void onMessage(MassQuote message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("Mass quote " + message.getNoQuoteSets().getValue());
        }

        @Override
        public void onMessage(MassQuoteAcknowledgement message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {
            log.info("Mass quote acknowledgement");

            int quoteStatus = message.getQuoteStatus().getValue();
            String quoteReqId = message.get(new QuoteReqID()).getValue();
            String symbol = message.getField(new Symbol()).getValue();

            log.info(symbol + ":" + quoteReqId + "->" + quoteStatus);
        }

        @Override
        public void onMessage(TradingSessionStatus message, SessionID sessionID) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue {

        }

        public List<String> getInstrumentsList() {
            return new ArrayList<>(availableSymbols);
        }
    }

    private SocketInitiator sInitiator;
    private App fixConnector;
    private IDGenerator idGenerator;
    private static final Logger log = LoggerFactory.getLogger(SmartTradeFixTest.class);

    private SessionSettings initSessionSettingsFromFile(String config) throws ConfigError, IOException {
        InputStream inputStream = SmartTradeFixTest.class.getResourceAsStream(config);
        if (inputStream == null) {
            try {
                inputStream = new FileInputStream(config);
            } catch (FileNotFoundException e) {
                log.error(String.format("Wrong config file %s", config));
            }
        }

        if (inputStream == null) {
            throw new IllegalArgumentException(String.format("Can't load config from %s", config));
        }

        SessionSettings settings = new SessionSettings(inputStream);
        inputStream.close();
        return settings;
    }

    public void wait(int msec) {
        try {
            Thread.sleep(msec);
        } catch (Exception ignored) {
        }
    }

    public void runTestConnection(String sessionConfigFilePath, TestType testType, String... symbols) {
        SessionSettings settings;
        fixConnector = new App();
        idGenerator = new IDGenerator();
        try {
            settings = initSessionSettingsFromFile(sessionConfigFilePath);
            log.info("Trying to connect ...");

            sInitiator = new SocketInitiator(
                    fixConnector,
                    new MemoryStoreFactory(),
                    settings,
                    new FixLogFactory(settings),
                    new DefaultMessageFactory());

            sInitiator.start();

            // wait for logon message
            wait(2000);

            SessionID marketSessionId = null;
            SessionID tradeSessionId = null;
            for (SessionID sid : sInitiator.getSessions()) {
                log.info("Session: " + sid.toString());
                if (sid.getSenderCompID().startsWith("TRD")) {
                    tradeSessionId = sid;
                } else {
                    marketSessionId = sid;
                }
            }

            switch (testType) {
                case ORDER:
                    // Trading Session Status Request
                    TradingSessionStatusRequest tsr = new TradingSessionStatusRequest(
                            new TradSesReqID(idGenerator.getClOrdId()),
                            new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT)
                    );

                    log.info("Sending TradingSessionStatusRequest ... ");
                    fixConnector.send(tsr, tradeSessionId);
                    wait(2000);
                    testNewOrderSingle(fixConnector, tradeSessionId, symbols);
                    wait(2000);
                    break;
                case MARKET_DATA:
                    log.info("Sending testMarketDataRequest ... ");
                    testMarketDataRequest(fixConnector, marketSessionId, symbols);
                    break;
                case QUOTES:
                    log.info("Sending testQuotesRequest ... ");
                    testQuotes(fixConnector, 10.0, marketSessionId, symbols);
                    break;
                case SECURITY_LIST:
                    // Security list request
                    SecurityListRequest slr = new SecurityListRequest(
                            new SecurityReqID(idGenerator.getSecurityReqId()),
                            new SecurityListRequestType(SecurityListRequestType.ALL_SECURITIES));

                    log.info("Sending SecurityListRequest ... ");
                    fixConnector.send(slr, marketSessionId);
                    wait(2000);
                    System.out.println(">>> Instruments: " + fixConnector.getInstrumentsList());
                    break;
            }
            
            wait(MARKET_DATA_LISTENING_INTERVAL_MS);
            log.info("done");

            sInitiator.stop(true);

            // after finish we save received market data to txt file for analysis
            for (String symbol : symbols) {
                List<Message> messages = marketMessages.get(symbol);
                if (messages != null) {
                    String mlogFile = MARKET_DATA_FILE_PREFIX + symbol.replace("/", "_").toLowerCase() + ".txt";
                    log.info("Writing collected messages to '" + mlogFile + "'");
                    PrintWriter pw = new PrintWriter(new FileOutputStream(mlogFile));
                    for (Message message : messages) {
                        pw.println(message.toRawString().replaceAll("\\001", "|"));
                    }
                    pw.close();
                }
            }
        } catch (ConfigError | IOException configError) {
            configError.printStackTrace();
        }
    }

    private void testNewOrderSingle(App fixConnector, SessionID sessionID, String... symbols) {
        for (String symbol : symbols) {
            NewOrderSingle newOrderSingle = new NewOrderSingle();
            newOrderSingle.set(new ClOrdID(idGenerator.getClOrdId()));
            newOrderSingle.set(new Symbol(symbol));
            newOrderSingle.set(new Side(Side.BUY));
            newOrderSingle.set(new TransactTime(LocalDateTime.now(ZoneOffset.UTC)));
            newOrderSingle.set(new OrderQty(1.0));
            newOrderSingle.set(new OrdType(OrdType.MARKET));
            newOrderSingle.set(new TimeInForce(TimeInForce.GOOD_TILL_CANCEL));
            newOrderSingle.set(new OrderCapacity(OrderCapacity.PRINCIPAL));
            fixConnector.send(newOrderSingle, sessionID);
        }
    }

    private void testMarketDataRequest(App fixConnector, SessionID sessionID, String... symbols) {
        for (String symbol : symbols) {
            MarketDataRequest mdRequest = new MarketDataRequest();
            mdRequest.set(new MDReqID(idGenerator.getMdReqId()));
            mdRequest.set(new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT_UPDATES));
            mdRequest.set(new MarketDepth(0));
            mdRequest.set(new MDUpdateType(MDUpdateType.INCREMENTAL_REFRESH));
            MarketDataRequest.NoRelatedSym noRelatedSym = new MarketDataRequest.NoRelatedSym();
            noRelatedSym.set(new Symbol(symbol));
            mdRequest.addGroup(noRelatedSym);
            fixConnector.send(mdRequest, sessionID);
        }
    }

    private void testQuotes(App fixConnector, double orderQty, SessionID sessionID, String... symbols) {

        Map<String, String> symbolToQuoteReqIds = new HashMap<>();
        for (String symbol : symbols) {
            String quoteReqId = idGenerator.getQuoteReqId();
            QuoteRequest qRequest = new QuoteRequest();
            qRequest.set(new QuoteReqID(quoteReqId));
            QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();
            noRelatedSym.set(new Symbol(symbol));
            noRelatedSym.set(new Side('0'));
            noRelatedSym.set(new OrderQty(orderQty));
            qRequest.addGroup(noRelatedSym);
            qRequest.set(new OrderCapacity(OrderCapacity.PRINCIPAL));
            fixConnector.send(qRequest, sessionID);
            symbolToQuoteReqIds.put(symbol, quoteReqId);
        }

        wait(2000);

        for (String symbol : symbols) {
            String quoteReqId = symbolToQuoteReqIds.get(symbol);
            QuoteCancel qCancel = new QuoteCancel();
            qCancel.set(new QuoteReqID(quoteReqId));
            qCancel.setField(new Symbol(symbol));
            fixConnector.send(qCancel, sessionID);
        }
    }

    public enum TestType {
        SECURITY_LIST,
        ORDER,
        MARKET_DATA,
        QUOTES
    }

    private static final String[] TEST_INSTRUMENTS = {"GBP/USD"};
    private static final String SESSION_CONFIG_FILE = "test.cfg";
    private static final String MARKET_DATA_FILE_PREFIX = "market_data_";
    private static final int MARKET_DATA_LISTENING_INTERVAL_MS = 60000;

    public static void main(String[] args) {
        new SmartTradeFixTest().runTestConnection(SESSION_CONFIG_FILE, TestType.MARKET_DATA, TEST_INSTRUMENTS);
    }
}