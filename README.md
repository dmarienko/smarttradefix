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

Also it's possible to run small python FIX cracker script to build aggregated Order Book on collected market data updates.
From folder FixMessageParser run following command: `python FIXCracker.py <path_to_your_collected_data_txt>` :
For example `python FIXCracker.py data/market_data_gbp_usd.txt` it will show aggregated order book for every market update.

Example:
![Application example](https://raw.githubusercontent.com/dmarienko/smarttradefix/master/FixMessagesParser/images/running_app.png)


