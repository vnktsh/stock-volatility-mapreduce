import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockVolatility {

	public static void main(String[] args) {

		try {
			long start = new Date().getTime();
			// System.out.println("started***************");
			// Create a new Job
			Path firstPath = new Path("FirstPath");
			// Path secPath = new Path("SecondPath");
			Job job = Job.getInstance();
			job.setJarByClass(StockVolatility.class);

			job.setMapperClass(FirstMapper.class);
			job.setReducerClass(FirstReducer.class);

			// job.setNumReduceTasks(5);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, firstPath);

			// MultipleOutputs.addNamedOutput(job, "FirstPa", TextOutputFormat.class,
			// Text.class, DoubleWritable.class);

			// job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setJarByClass(StockVolatility.class);
			job.waitForCompletion(true);

			Job secJob = Job.getInstance();
			secJob.setJarByClass(StockVolatility.class);

			secJob.setMapperClass(SecondMapper.class);
			secJob.setReducerClass(SecondReducer.class);

			secJob.setMapOutputKeyClass(NullWritable.class);
			secJob.setMapOutputValueClass(Text.class);

			secJob.setOutputKeyClass(Text.class);
			secJob.setOutputValueClass(NullWritable.class);

			// secJob.setInputFormatClass(SequenceFileInputFormat.class);

			FileInputFormat.addInputPath(secJob, firstPath);
			FileOutputFormat.setOutputPath(secJob, new Path(args[1]));
			secJob.waitForCompletion(true);

			long end = new Date().getTime();
			System.out.println("Time Taken " + (end - start) / (1000));

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
