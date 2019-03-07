/**
 * ----------------------------
 * SmartTrade connector testing
 * (c) 2019, AppliedAlpha
 * http://www.AppliedAlpha.com/
 */
package com.aa.ira.broker.smarttrade.fix;

import quickfix.FileLogFactory;
import quickfix.Log;
import quickfix.SessionID;
import quickfix.SessionSettings;

import java.io.Closeable;
import java.io.IOException;


/**
 * Created by IntelliJ IDEA.
 * Date: 01.04.14
 * Time: 18:02
 */
public class FixLogFactory extends FileLogFactory {

    public FixLogFactory(SessionSettings settings) {
        super(settings);
    }

    @Override
    public Log create(SessionID sessionID) {
        return new LogWrapper(sessionID, super.create(sessionID));
    }

    private class LogWrapper implements Log, Closeable {
        private Log log;
        private SessionID sessionID;

        LogWrapper(SessionID sessionID, Log log) {
            this.sessionID = sessionID;
            this.log = log;
        }

        @Override
        public void clear() {
            log.clear();
        }

        @Override
        public void onIncoming(String message) {
            log.onOutgoing(logMessage(message, "incoming"));
        }

        @Override
        public void onOutgoing(String message) {
            log.onOutgoing(logMessage(message, "outgoing"));
        }

        @Override
        public void onEvent(String text) {
            log.onEvent(logMessage(text, "event"));
        }

        @Override
        public void onErrorEvent(String text) {
            log.onErrorEvent(logMessage(text, "error"));
        }

        @Override
        public void close() throws IOException {
            if (log instanceof Closeable) {
                ((Closeable) log).close();
            }
        }

        private String logMessage(String message, String type) {
            String sessionIdName = String.format("%s.%s.%s", sessionID.getSenderCompID(), sessionID.getTargetCompID(), type);
            return "" + sessionIdName + ": " + message;
        }
    }
}
