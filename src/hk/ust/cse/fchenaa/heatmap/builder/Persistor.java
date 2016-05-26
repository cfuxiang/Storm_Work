package hk.ust.cse.fchenaa.heatmap.builder;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.code.geocoder.model.LatLng;

import redis.clients.jedis.Jedis;

public class Persistor extends BaseBasicBolt {
	private final Logger logger = LoggerFactory.getLogger(Persistor.class);
	//private Jedis jedis; //let's not use redis also, we will just write to a file
	//private ObjectMapper objectMapper; //let's not use the Jackson's JSON Mapper class. Version issues.

	@Override
	public void prepare(Map stormConf,
			TopologyContext context) {
		//jedis = new Jedis("localhost");
		//objectMapper = new ObjectMapper();
	}
	
	@Override
	public void execute(Tuple tuple,
			BasicOutputCollector outputCollector) {
		Long timeInterval = tuple.getLongByField("time-interval");
		//List<LatLng> hz = (List<LatLng>) tuple.getValueByField("hotzones");
		//List<String> hotzones = asListOfStrings(hz);
		List<String> hotzones = (List<String>) tuple.getValueByField("hotzones");
		try {
			String key = "checkins-" + timeInterval;
			//String value = objectMapper.writeValueAsString(hotzones);
			String value = hotzones.toString();
			//jedis.set(key, value);
			//save it to a file instead
			try {
			    Files.write(Paths.get("redis-data.txt"), "the text".getBytes(), StandardOpenOption.APPEND);
			}catch (IOException e) {
			    //exception handling left as an exercise for the reader
			}
		} catch (Exception e) {
			logger.error("Error persisting for time: " + timeInterval, e);
		}
	}
	
	/*
	private List<String> asListOfStrings(List<LatLng> hotzones) {
		List<String> hotzonesStandard = new ArrayList<String>(hotzones.size());
		for (LatLng geoCoordinate : hotzones) {
			hotzonesStandard.add(geoCoordinate.toUrlValue());
		}
		return hotzonesStandard;
	}
	*/
	
	@Override
	public void cleanup() {
		//if (jedis.isConnected()) {
		//	jedis.quit();
		//}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// No output fields to be declared
	}
}
