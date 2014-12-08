/**
 * 
 * @author Pavan
 * 
 * This program gets the n grams file and writes the probabilities of the next 5 words for each phrase
 * 
 */
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;

@SuppressWarnings("deprecation")
public class TextPrediction {

	public static class PredictMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// get the line from the input file
			String line = value.toString();

			//system.out.println("Mapper");
			// split the line into words into ngram and count
			// get the number of words in the ngram
			String[] wordsintheNGram = line.split("\t")[0].split(" ");
			int len = wordsintheNGram.length;

			// get the count
			int cnt = Integer.parseInt(line.split("\t")[1]);
			 //system.out.println("mapper count " + cnt);

			//system.out.println("mapper lenght " + len);
			// Dont consider words which are less than 2 gram and also the words
			// which occured only once
			if (len > 1 && cnt > 1) {

				// the last word and the count of it will be output my mapper
				String columnword = wordsintheNGram[len - 1];
				//system.out.println("nextWord " + columnword);

				// output the count and the last word of the n gram
				String columnwordCount = columnword + " " + cnt;

				// hbase row key needs to be built
				String hbaserowkey = "";
				// remove the last word and attach all other words to this key

				for (int i = 0; i < len - 1; i++) {
					hbaserowkey = hbaserowkey + " " + wordsintheNGram[i];
				}

				// remove all the leading and trailing spaces
				hbaserowkey = hbaserowkey.trim();

				//system.out.println("rowkey :" + hbaserowkey + ":");

				////system.out.println("necxt word count  " + nextWordCount);
				word.set(hbaserowkey);

				output.collect(word, new Text(columnwordCount));
			}

		}
	}

	public static class PredictReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private HTable table;

		private Configuration conf = HBaseConfiguration.create();
	
		public void configure(JobConf job) {
			try {
				// get an object to table 'q' which should be manually created
				// on the hbase
				table = new HTable(conf, "t");
				// setting autoflush to false doesnot slow down the process of
				// putting values into hbase

				table.setAutoFlush(false);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			try {
				// Store the column words and its count
				Map<String, Integer> columnwordandcounts = new HashMap<String, Integer>();

				 //system.out.println("Reducer");
				int denominatorsum = 0;
				 //system.out.println("Values "+ values);
				while (values.hasNext()) {

					String w = values.next().toString();

					// get the word that needs to be made a column and also the
					// corresponding count
					String[] columnwordCount = w.split(" ");
					// sum the wordcounts of the all the columns for that
					// particular key
					denominatorsum += Integer.parseInt(columnwordCount[1]);

					columnwordandcounts.put(columnwordCount[0],
							Integer.parseInt(columnwordCount[1]));

					 //system.out.println("reducer " + columnwordCount[0] + ":::" +
							// columnwordCount[1]);

				}

				// sort the words based on their counts before inserting them
				// into hbase

				List<Map.Entry<String, Integer>> columnwordsList = new ArrayList<Map.Entry<String, Integer>>(
						columnwordandcounts.entrySet());
				// sorting keys based on the values
				Collections.sort(columnwordsList,
						new Comparator<Map.Entry<String, Integer>>() {
							public int compare(Map.Entry<String, Integer> o1,
									Map.Entry<String, Integer> o2) {
								return (o2.getValue() - o1.getValue());
							}
						});

				// put the row key
				Put put = new Put(Bytes.toBytes(key.toString()));
				 //system.out.println("KEY :"+key+":");
				int i = 0;

				for (Map.Entry<String, Integer> s : columnwordsList) {

					// put the column word and its probability
					put.add(Bytes.toBytes("p"),
							Bytes.toBytes(s.getKey()),
							Bytes.toBytes((String.valueOf(s.getValue()
									/ (float) denominatorsum))));

					//system.out.println("reducer key  :" + s.getKey() + ":");
					i++;

					// get only the top 5 words
					if (i == 6)
						break;

				}
				// insert into table
				table.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		public void close() {
			try {
				// commit the puts as autoflush is set to false
				table.flushCommits();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TextPrediction.class);
		conf.setJobName("TextPrediction");

		// set outputkey and value classes
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// set input and output formats
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TableOutputFormat.class);

		// set mapper and reducer classes
		conf.setMapperClass(PredictMap.class);
		conf.setReducerClass(PredictReduce.class);

		//set the input to the ngram count file
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
