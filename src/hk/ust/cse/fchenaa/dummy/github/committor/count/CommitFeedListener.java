package hk.ust.cse.fchenaa.dummy.github.committor.count;

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

public class CommitFeedListener extends BaseRichSpout {
	private SpoutOutputCollector outputCollector;
	private List<String> commits;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("commit"));
	}
	@Override
	public void open(Map configMap,
			TopologyContext context,
			SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		try {
			commits = IOUtils.readLines(
					ClassLoader.getSystemResourceAsStream("commitor.txt"),
					Charset.defaultCharset().name()
					);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	@Override
	public void nextTuple() {
		for (String commit : commits) {
			outputCollector.emit(new Values(commit));
		}
	}
}