package hk.ust.cse.fchenaa.heatmap.builder;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

public class LocalTopologyRunner {
	public static void main(String[] args) {
		Config config = new Config();
		StormTopology topology = HeatmapTopologyBuilder.build();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-heatmap", config, topology);
	}
}