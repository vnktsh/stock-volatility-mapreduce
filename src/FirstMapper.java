import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();

		if (line.contains(",") && !line.contains("Date")) {
			String[] arr = line.split(",");
			if (arr[0].contains("-")) {
				String[] dtArr = arr[0].split("-");
				String date = dtArr[0] + dtArr[1] + "-" + dtArr[2];
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				String adjClose = arr[arr.length - 1];
				Text valTx = new Text(date + "-" + adjClose);
				Text ketTx = new Text(fileName);
				try {
					context.write(ketTx, valTx);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
