package hk.ust.cse.fchenaa.heatmap.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.code.geocoder.model.LatLng;

public class HeatMapBuilder extends BaseBasicBolt {

	private Map<Long, List<String>> heatmaps; //changed <LatLng> to <String> 
	
	@Override
	public void prepare(Map config,
			TopologyContext context) {
		heatmaps = new HashMap<Long, List<String>>();
	}
	
	private Long selectTimeInterval(Long time) {
		return time / (15 * 1000);
	}
	
	private List<String> getCheckinsForInterval(Long timeInterval) {
		List<String> hotzones = heatmaps.get(timeInterval);
		if (hotzones == null) {
			hotzones = new ArrayList<String>();
			heatmaps.put(timeInterval, hotzones);
		}
		return hotzones;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
		return conf;
	}

	@Override
	public void execute(Tuple tuple,
			BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			emitHeatmap(outputCollector);
		} else {
			Long time = tuple.getLongByField("time");
			//LatLng geocode = (LatLng) tuple.getValueByField("geocode");
			String geocode = (String) tuple.getValueByField("geocode"); //let's use back the "geocode" naming to avoid huge breakage in the code
			Long timeInterval = selectTimeInterval(time);
			List<String> checkins = getCheckinsForInterval(timeInterval);
			checkins.add(geocode);
		}
	}

	private boolean isTickTuple(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
				&& sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}


	private void emitHeatmap(BasicOutputCollector outputCollector) {
		Long now = System.currentTimeMillis();
		Long emitUpToTimeInterval = selectTimeInterval(now);
		Set<Long> timeIntervalsAvailable = heatmaps.keySet();
		for (Long timeInterval : timeIntervalsAvailable) {
			if (timeInterval <= emitUpToTimeInterval) {
				List<String> hotzones = heatmaps.remove(timeInterval);
				outputCollector.emit(new Values(timeInterval, hotzones));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
		// TODO Auto-generated method stub
		fieldsDeclarer.declare(new Fields("time-interval", "hotzones"));
	}

}
