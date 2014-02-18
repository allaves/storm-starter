package storm.starter.bolt;

import java.io.IOException;
import java.util.Map;

import javax.websocket.Endpoint;

import storm.starter.tools.WebSocketEndpoint;
import storm.starter.util.MessageHandler;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DisplayHSLVehiclesBolt implements IRichBolt {
	
	private OutputCollector collector;
	//private WebSocketEndpoint endpoint;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;	
		//this.endpoint = ...
	}

	@Override
	public void execute(Tuple input) {
		//System.out.println("Lat/lon: " + input.getStringByField("lat") + " " + input.getStringByField("lon"));
		//Send a message with the coordinates to a queue (one queue per vehicle or per vehicle type?)
		try {
			MessageHandler.push(MessageHandler.createChannel(), input.getStringByField("vehicleId") + " " + input.getStringByField("lat") + " " + input.getStringByField("lon"), input.getStringByField("vehicleId"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("lat", "lon"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
