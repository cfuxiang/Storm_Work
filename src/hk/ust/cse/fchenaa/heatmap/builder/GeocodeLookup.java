package hk.ust.cse.fchenaa.heatmap.builder;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

public class GeocodeLookup extends BaseBasicBolt {
	private Geocoder geocoder;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
		fieldsDeclarer.declare(new Fields("time", "geocode"));
	}

	@Override
	public void prepare(Map config,
			TopologyContext context) {
		geocoder = new Geocoder();
	}

	@Override
	public void execute(Tuple tuple,
			BasicOutputCollector outputCollector) {
		String address = tuple.getStringByField("address");
		Long time = tuple.getLongByField("time");

		//let's not use google's geocoder to find the lat and long values as there's some issues
		//instead, let's just do a direct forward of the address
		/*
		GeocoderRequest request = new GeocoderRequestBuilder()
				.setAddress(address)
				.setLanguage("en")
				.getGeocoderRequest();
		GeocodeResponse response =  null;
		try {
			response = geocoder.geocode(request);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GeocoderStatus status = response.getStatus();

		if (GeocoderStatus.OK.equals(status)) {
			GeocoderResult firstResult = response.getResults().get(0);
			LatLng latLng = firstResult.getGeometry().getLocation();
			outputCollector.emit(new Values(time, latLng));
		}
		*/
		outputCollector.emit(new Values(time, address));
	}
}