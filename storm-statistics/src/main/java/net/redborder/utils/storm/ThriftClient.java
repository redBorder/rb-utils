package net.redborder.utils.storm;

import backtype.storm.generated.Nimbus.Client;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;

public class ThriftClient {
	// IP of the Storm UI node
	private   String STORM_UI_NODE = "127.0.0.1";

    public ThriftClient(String storm_ui){
        STORM_UI_NODE=storm_ui;
    }

	public Client getClient() {
		  // Set the IP and port of thrift server.
		  TSocket socket = new TSocket(STORM_UI_NODE, 6627);
		  TFramedTransport tFramedTransport = new TFramedTransport(socket);
		  TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		  Client client = new Client(tBinaryProtocol);
		  try {
			  // Open the connection with thrift client.
			  tFramedTransport.open();
		  }catch(Exception exception) {
			  throw new RuntimeException("Error occurred while making connection with nimbus thrift server");
		  }
		  // return the Nimbus Thrift client.
		  return client;		  
	}
}
