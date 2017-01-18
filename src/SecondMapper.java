import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private TreeMap<Double, String> highestVolatilityStocks = new TreeMap<Double, String>();
	private TreeMap<Double, String> leastVolatilityStocks = new TreeMap<Double, String>();

	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString().trim();
		if (line != null) {
			int index = line.indexOf(".csv");
			String stockName = line.substring(0, index);
			double stockVal = Double.parseDouble(line.substring(index + 4).trim());
			double precision = Math.random() * (0.00001);
			if (highestVolatilityStocks.containsKey(stockVal)) {
				stockVal = stockVal + precision;
			}
			highestVolatilityStocks.put(stockVal, stockName);
			if (highestVolatilityStocks.size() > 10) {
				highestVolatilityStocks.remove(highestVolatilityStocks.firstKey());
			}

			if (leastVolatilityStocks.containsKey(stockVal)) {
				precision = Math.random() * (0.00001);
				stockVal = stockVal + precision;
			}

			leastVolatilityStocks.put(stockVal, stockName);

			if (leastVolatilityStocks.size() > 10) {
				leastVolatilityStocks.remove(leastVolatilityStocks.lastKey());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// System.out.println("highestVolatilityStocks#########" + highestVolatilityStocks);
		// System.out.println("leastVolatilityStocks##### " + leastVolatilityStocks);

		for (Double key : highestVolatilityStocks.keySet()) {
			Text value = new Text(highestVolatilityStocks.get(key) + "#" + key);

			context.write(NullWritable.get(), value);
		}

		for (Double key : leastVolatilityStocks.keySet()) {
			Text value = new Text(leastVolatilityStocks.get(key) + "#" + key);
			context.write(NullWritable.get(), value);
		}
	}

}