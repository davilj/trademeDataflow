package org.davilj.trademe.dataflow.reports.aggregate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;

public class DailyStats extends CombineFn<Integer, DailyStats.Stats, String> {
	
		public static class Stats {
		int sum = 0;
		int count = 0;
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;

		public void checkMinMax(Integer input) {
			min = (input < min) ? input : min;
			max = (input > max) ? input : max;
		}

		@Override
		public String toString() {
			return String.format("%s|%s|%s|%s", sum, count, min, max);
		}
		

		public static Stats readFromStream(InputStream in) {
			Stats stats = new Stats();
			
			DataInputStream dataIn = new DataInputStream(in);
			try {
				stats.sum = dataIn.readInt();
				stats.count = dataIn.readInt();
				stats.max = dataIn.readInt();
				stats.min = dataIn.readInt();
			} catch (IOException e) {
				throw new RuntimeException();
			}
			return stats;
		}

		public void writeToSteam(OutputStream out) {
			DataOutputStream dataOut = new DataOutputStream(out);
			try {
				dataOut.writeInt(this.count);
				dataOut.writeInt(this.sum);
				dataOut.writeInt(this.max);
				dataOut.writeInt(this.min);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public Stats createAccumulator() {
		return new Stats();
	}

	@Override
	public Stats addInput(Stats accum, Integer input) {
		accum.sum += input;
		accum.count++;
		accum.checkMinMax(input);
		return accum;
	}

	@Override
	public Stats mergeAccumulators(Iterable<Stats> stats) {
		Stats merged = createAccumulator();
		for (Stats statInfo : stats) {
			merged.sum += statInfo.sum;
			merged.count += statInfo.count;
			merged.min = merged.min < statInfo.min ? merged.min : statInfo.min;
			merged.max = merged.max > statInfo.max ? merged.max : statInfo.max;
		}
		return merged;
	}

	@Override
	public String extractOutput(Stats stats) {
		return stats.toString();
	}

	public static Class<?> getCoder() {
		return StatsCoder.class;
	}
	
	static class StatsCoder extends CustomCoder<Stats> {
		
		public static StatsCoder of() {return new StatsCoder();}

		@Override
		public void encode(Stats value, OutputStream outStream,
				com.google.cloud.dataflow.sdk.coders.Coder.Context context) throws CoderException, IOException {
			value.writeToSteam(outStream);
		}

		@Override
		public Stats decode(InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
				throws CoderException, IOException {
			return Stats.readFromStream(inStream);
		}
	}
	
}
