package storm.starter;

import storm.starter.bolt.DisplayHSLVehiclesBolt;
import storm.starter.bolt.SeparateLinesBolt;
import storm.starter.spout.HSLObservationsSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class HSLVehicleTrackingTopology {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("hslService", new HSLObservationsSpout(), 1);
		builder.setBolt("separateLines", new SeparateLinesBolt(), 1).shuffleGrouping("hslService");
		builder.setBolt("separateFields", new SeparateFieldsBolt(), 8).shuffleGrouping("separateLines");
		builder.setBolt("displayObservations", new DisplayHSLVehiclesBolt(), 8).fieldsGrouping("separateFields", new Fields("vehicleId"));
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hslVehicleTracking", conf, builder.createTopology());
		Utils.sleep(40000);
		cluster.killTopology("hslVehicleTracking");
		cluster.shutdown();
	}
}
