/**
 * This is a map reduce program that implements Inverted index 
 * 
 *  @author Pavan
 */
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;

@SuppressWarnings("deprecation")
public class Inverted_index {

	/**
	 * This is the mapper class that splits the words and also finds the file
	 * that the word is coming from
	 * 
	 * @author Pavan
	 * 
	 */
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		// store the stop words
		public HashSet<String> stopwordslist = new HashSet<String>();

		// file path where the stop words need to be cached
		public Path[] filePath;

		/**
		 * configure method is run before the mapper job by default
		 * 
		 * @param job
		 * 
		 */
		public void configure(JobConf job) {
			try {
				// get the distributed cache which is local to respective
				// machine
				filePath = DistributedCache.getLocalCacheFiles(job);
				File cacheFile = new File(filePath.toString());
				Scanner scanner = new Scanner(cacheFile);
				String stopWord;
				// scan every line and put the stop words in a list
				while (scanner.hasNextLine()) {
					// remove any extra spaces
					stopWord = scanner.nextLine().trim();
					stopwordslist.add(stopWord);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * map will tokenise and write to standard output before
		 * 
		 * 
		 * 
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// this is to get the file name of the chunk
			FileSplit fs = (FileSplit) reporter.getInputSplit();
			String filename = fs.getPath().getName();

			Text word = new Text();
			// get the line
			String line = value.toString();
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(line);

			// parse all the tokens
			while (tokenizer.hasMoreTokens()) {
				String token = new String();
				token = tokenizer.nextToken();
				//replace all non chars by space
				token = token.toLowerCase().replaceAll("[^a-z]", " ");
				//this is sca
				if (stopwordslist.contains(token) || token.isEmpty()) {
					continue;
				}
				word.set(new Text(token));
				output.collect(word, new Text(filename));
			}
		}
	}

	/**
	 * 
	 * this is the reducer class which had reduce functions
	 * 
	 * @author Pavan
	 * 
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/**
		 * This is the reducer function which takes key values and combines them
		 * accordingly
		 */
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			HashSet<String> record = new HashSet<String>();
			String filenamesCSV = "";
			while (values.hasNext()) {

				String filename = values.next().toString();
				// this is to remove duplicate entries for the word
				if (record.contains(filename) || filename.isEmpty())
					continue;
				else {
					if (filenamesCSV.isEmpty())
						filenamesCSV = filename;
					else
						filenamesCSV = filenamesCSV + "," + filename;

					record.add(filename);
				}
			}
			// write the reduced output
			output.collect(key, new Text(filenamesCSV));
		}
	}

	/**
	 * needs input location and output location as the command line arguments
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// created a job
		JobConf conf = new JobConf(Inverted_index.class);
		conf.setJobName("inverted_Index");

		// creating a stop words file which can be on distributed cache and be
		// accessed on the URL
		Path path = new Path("/stopme/stopwords.txt");
		DistributedCache.addCacheFile(path.toUri(), conf);

		// set the output key class type
		conf.setOutputKeyClass(Text.class);
		// set the output value classs type
		conf.setOutputValueClass(Text.class);

		// set the mappper class
		conf.setMapperClass(Map.class);
		// combiner is used to decrease the amount of data processed by reducers
		// and for this both combiner and reducer are same class
		conf.setCombinerClass(Reduce.class);
		// set the reducer job
		conf.setReducerClass(Reduce.class);

		// setting the input format
		conf.setInputFormat(TextInputFormat.class);
		// setting the standard output format
		conf.setOutputFormat(TextOutputFormat.class);

		// set the location of the input
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// set the location of the output
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// start the job
		JobClient.runJob(conf);
	}
}
