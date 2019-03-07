# SmartTrade FIX connector example

SmartTrade FIX client example

Before run this example you need to setup following settings in ClientExample/src/main/resources/com/aa/ira/broker/smarttrade/test.cfg file:

```
	[session]
	SocketConnectPort=<set data server port>
	SocketConnectHost=<set data server address>
	SenderCompID=<data sender ID >
	TargetCompID=LFX
	SessionQualifier=FEED
	....

	[session]
	SocketConnectPort=<set trading server port>
	SocketConnectHost=<set data server address>
	SenderCompID=<trading sender ID >
	TargetCompID=LFX
	SessionQualifier=BROKER
```

After it you can compile project using maven from ClientExample directory (`mvn clean install`) and then run it by ./runclient.bat script.

If will connect to server and ask for market data incremental updates. All received market data messages will be stored to text file (there is example at ClientExample/market_data_gbp_usd.txt). All FIX messages will be stored to ClientExample/logs folder.


