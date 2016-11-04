package org.davilj.trademe.dataflow.reports;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.Coder;
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

		public static Stats createFromString(String data) {
			String[] parts = data.split("\\|");
			System.err.println(data + " - ");
			Stats stats = new Stats();
			if (data.trim().isEmpty()) return stats;
			stats.sum = Integer.parseInt(extractNumberString(parts[0]));
			stats.count = Integer.parseInt(extractNumberString(parts[1]));
			stats.min = Integer.parseInt(extractNumberString(parts[2]));
			stats.max = Integer.parseInt(extractNumberString(parts[3]));
			return stats;
		}
		
		private static String extractNumberString(String str) {
			String trimmed = str.trim();
			if (trimmed.isEmpty()) {
				return "0";
			} else {
				return trimmed;
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
			byte[] bytes = value.toString().getBytes();
			outStream.write(bytes);
		}

		@Override
		public Stats decode(InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
				throws CoderException, IOException {
			int i=-1;
			int index=0;
			byte[] bytes = new byte[128];
			while ((i = inStream.read())!=-1) {
				bytes[index++]=(byte)i;
			}
			
			String data = new String(bytes);
			return Stats.createFromString(data);
		}
	}
	
}
