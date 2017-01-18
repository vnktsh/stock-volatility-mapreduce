import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
	private TreeMap<Double, String> highestVolatilityStocks = new TreeMap<Double, String>();
	private TreeMap<Double, String> leastVolatilityStocks = new TreeMap<Double, String>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] arr = value.toString().split("#");
			String stockName = arr[0];
			double stockVal = Double.parseDouble(arr[1]);

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
		// System.out.println("highestVolatilityStocks*** " + highestVolatilityStocks);
		// System.out.println("leastVolatilityStocks*** " + leastVolatilityStocks);
		context.write(new Text("Top 10 stocks with the highest volatility (Descending order)"), NullWritable.get());
		for (double stockVal : highestVolatilityStocks.descendingKeySet()) {
			context.write(new Text(highestVolatilityStocks.get(stockVal)), NullWritable.get());
		}
		context.write(new Text("Top 10 stocks with the Least volatility (Aescending order)"), NullWritable.get());
		for (double stockVal : leastVolatilityStocks.keySet()) {
			context.write(new Text(leastVolatilityStocks.get(stockVal)), NullWritable.get());
		}
	}
}
