package hk.ust.cse.fchenaa.heatmap.builder;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class HeatmapTopologyBuilder {
	
	public static StormTopology build() {
		TopologyBuilder builder = new TopologyBuilder();
		builder	.setSpout("checkins", new Checkins(), 4);
		builder	.setBolt("geocode-lookup", new GeocodeLookup(), 8) 	//# of executors
				.setNumTasks(8)										//# of tasks (overridden, if not it will use the same value as the # of executors above)
				.shuffleGrouping("checkins");
		builder	.setBolt("heatmap-builder", new HeatMapBuilder())
				.globalGrouping("geocode-lookup");
		builder	.setBolt("persistor", new Persistor())
				.shuffleGrouping("heatmap-builder");
		return builder.createTopology();
	}
}
