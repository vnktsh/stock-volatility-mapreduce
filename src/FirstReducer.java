import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// System.out.println("key*** " + key);
		Iterator<Text> it = values.iterator();
		String startVal = it.next().toString();
		double startMonthVal = Double.parseDouble(startVal.split("-")[0]);
		double startCloseval = Double.parseDouble(startVal.split("-")[2]);
		int count = 1;
		ArrayList<Double> list = new ArrayList<Double>();
		double avg = 0;
		String prev = null;
		double diff = 0, endCloseVal = 0;
		while (it.hasNext()) {
			String str = it.next().toString();
			int nextMonthYear = Integer.parseInt(str.split("-")[0]);
			// System.out.println("nextMonthYear " + nextMonthYear + " endMonthYear " +
			// endMonthYear);
			if (nextMonthYear == startMonthVal) {
				// System.out.println("nextMonthYear " + nextMonthYear);
				prev = str;
				continue;
			} else {
				// System.out.println("prev::: " + prev);
				if (prev == null) {
					diff = 0;
				} else {
					endCloseVal = Double.parseDouble(prev.split("-")[2]);
//					System.out.println("nextMonthYear " + nextMonthYear);
//					System.out.println("endCloseVal " + endCloseVal + " startCloseVal " + startCloseval);
					diff = (-startCloseval + endCloseVal) / startCloseval;
				}
				avg += diff;
				list.add(diff);
				count++;
				startMonthVal = nextMonthYear;
				startCloseval = Double.parseDouble(str.split("-")[2]);
			}
		}
		double volatility = 0;
//		System.out.println("avg " + avg);
		if (count != 1) {

//			System.out.println("startCloseval*** " + startCloseval);
			endCloseVal = Double.parseDouble(prev.split("-")[2]);
//			System.out.println("endCloseVal*** " + endCloseVal);
			diff = (-startCloseval + endCloseVal) / startCloseval;

			avg += diff;
			list.add(diff);
//			System.out.println("list " + list);
			// System.out.println("lprev " + prev);
//			System.out.println("avg " + avg);
//			System.out.println("count " + count);
			avg = avg / (count);

			for (double dbval : list) {
				volatility = volatility + (avg - dbval) * (avg - dbval);
			}

//			System.out.println("final volatility " + volatility);
			volatility = Math.sqrt(volatility / (count - 1));
			if (volatility > 0) {
				context.write(key, new DoubleWritable(volatility));
			}
		}

	}
}