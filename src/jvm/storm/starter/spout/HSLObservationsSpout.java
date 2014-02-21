package storm.starter.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HSLObservationsSpout implements IRichSpout {

	private SpoutOutputCollector collector;
	
	private URL hslUrl;
	private URLConnection connection;
	private BufferedReader br;
	
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			//this.hslUrl = new URL("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61");
			this.hslUrl = new URL("http://83.145.232.209:10001/?type=vehicle&id=RHKL00074");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		Utils.sleep(1000);
		try {
			this.connection = hslUrl.openConnection();
			this.connection.connect();
			this.br = new BufferedReader(new InputStreamReader(this.connection.getInputStream()));
			StringBuilder strBuilder = new StringBuilder();
			String line = null;
			while ((line = br.readLine()) != null) {
				strBuilder.append(line).append("\n");
			}
			this.collector.emit(new Values(strBuilder.toString()));
			this.br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		
	}
	
	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("vehicleObservations"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
