package hk.ust.cse.fchenaa.heatmap.builder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Checkins extends BaseRichSpout {
	private List<String> checkins;
	private int nextEmitIndex;
	private SpoutOutputCollector outputCollector;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "address"));
	}
	
	@Override
	public void open(Map config,
			TopologyContext topologyContext,
			SpoutOutputCollector spoutOutputCollector) {
		this.outputCollector = spoutOutputCollector;
		this.nextEmitIndex = 0;
		try {
			checkins =
					IOUtils.readLines(ClassLoader.getSystemResourceAsStream("checkins.txt"),
							Charset.defaultCharset().name());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void nextTuple() {
		String checkin = checkins.get(nextEmitIndex);
		String[] parts = checkin.split(",");
		Long time = Long.valueOf(parts[0]);
		String address = parts[1];
		outputCollector.emit(new Values(time, address));
		nextEmitIndex = (nextEmitIndex + 1) % checkins.size();
	}
}