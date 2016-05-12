package hk.ust.cse.fchenaa.trident;

import org.apache.storm.tuple.Values;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class FormatCall extends BaseFunction {
   @Override
   public void execute(TridentTuple tuple, TridentCollector collector) {
      String fromMobileNumber = tuple.getString(0);
      String toMobileNumber = tuple.getString(1);
      collector.emit(new Values(fromMobileNumber + " - " + toMobileNumber));
   }
}
