/**
 * This split line into 1 gram to 5 gram words and phrases.
 * @author Pavan
 */
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

@SuppressWarnings("deprecation")
public class NGramCount {
	/**
	 * This Map class will output the phrases.
	 * 
	 * @author Pavan
	 * 
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		/**
		 * This function will take the value which is the line and splits it to
		 * phrases.
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// get the line
			String line = value.toString();
			// replace all non alphabet into space and convert it to lowercase
			line = line.replaceAll("[^a-zA-Z ]", " ").toLowerCase();
			// take all the words now
			String[] ngrams = line.split("\\s+");

			int i = 0;
			// add one by one and output it
			while (i < ngrams.length) {

				// start with word pointed by i

				String startwithword = ngrams[i];
				word.set(startwithword);
				// put the collected word
				output.collect(word, one);
				// get the next word
				int nextwordindex = i + 1;
				// make combination of next 5 words with first word included
				for (int j = nextwordindex; j < i + 5 && j < ngrams.length; j++) {
					// combine with space and write it out
					startwithword = startwithword + " " + ngrams[j];
					word.set(startwithword);
					output.collect(word, one);
				}

				i++;

			}
		}
	}

	/**
	 * This reduces just counts all the phrases given out by the mapper
	 * 
	 * @author Pavan
	 * 
	 */

	public static class NGramReduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * this reducer function sums all the same type of keys and give out the
		 * count
		 */
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			// write it to standard out
			output.collect(key, new IntWritable(sum));
		}
	}

	/**
	 * main program which sets the map reduce job on the cluster
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(NGramCount.class);
		conf.setJobName("NGrams");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// set the mapper and reducer
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(NGramReduce.class);
		conf.setReducerClass(NGramReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// set the input and output paths
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		// run the job
		JobClient.runJob(conf);
	}
}
