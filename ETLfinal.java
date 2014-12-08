/**
 * @author Pavan
 * 
 * This is used to Extract the fields from JSON and load it on the HBASE tables
 * 
 */

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ETLfinal extends Configured implements Tool {

	static Configuration conf;

	public int run(String[] args) throws Exception {

		Job job = createSubmittableJob(conf, args);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("job:" + args[0]);
		conf = HBaseConfiguration.create();
		ETLfinal tc = new ETLfinal();
		tc.run(args);

	}
	
	/*
	 * This is the job that will extract out all the fields and give the values to hbase
	 */
	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {
		String pathStr = args[0];
		Path inputDir = new Path(pathStr);
		Job job = new Job(conf, "15619_Project");
		job.setJarByClass(ETLfinal.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setOutputFormatClass(MultiTableOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Extractor.class);
		job.setNumReduceTasks(0);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
		return job;
	}

	static class Extractor extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		/**
		 * This mapper will read the needed fields from json files
		 */
		@Override
		public void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {
			JSONParser parser = new JSONParser();
			String data = value.toString();

			Object obj;
			try {
				obj = parser.parse(data);

				JSONObject jsonObject = (JSONObject) obj;

				String created_at = (String) jsonObject.get("created_at");
				// System.out.println(convertdatformat(created_at));

				String TweetID = (String) jsonObject.get("id").toString();
				// System.out.println(TweetID);

				// parse user id

				JSONObject msg = (JSONObject) jsonObject.get("user");

				String UserID = (String) msg.get("id").toString();
				// System.out.println(UserID);

				String Tweet = (String) jsonObject.get("text");

				//for  q4
				String tweetTime = (String) jsonObject.get("created_at");
				String tweetID = (String) jsonObject.get("id").toString();
				String tweetText = (String) jsonObject.get("text").toString();

				// Table is tweetsbytime
				// Has 1 column family, named 't' which represents tweet text.
				// Tweet time becomes the primary key.

				// Key in the constructor.
				Put put = new Put(Bytes.toBytes(convertdatformat(tweetTime)));

				// put.add(Bytes.toBytes("ID"), Bytes.toBytes("TweetID"),
				// Bytes.toBytes(tweetID));
				put.add(Bytes.toBytes("t"), Bytes.toBytes(tweetID),
						Bytes.toBytes(tweetText));

				context.write(
						new ImmutableBytesWritable(Bytes
								.toBytes("tweetsbytime")), put);

				// get retweeted user id
				JSONObject msgretweet = (JSONObject) jsonObject
						.get("retweeted_status");
				Long retweeteduserID;

				if (msgretweet != null) {

					JSONObject retweeteduser = (JSONObject) msgretweet
							.get("user");
					retweeteduserID = (Long) retweeteduser.get("id");
					// System.out.println("RETWEETED ID " + retweeteduserID);

					/**
					 * Put put1 = new Put(
					 * Bytes.toBytes(retweeteduserID.toString()));
					 * put1.add(Bytes.toBytes("userid"),
					 * Bytes.toBytes("userid"), Bytes.toBytes(UserID));
					 */

					// for q3
					Put put1 = new Put(
							Bytes.toBytes(retweeteduserID.toString()));
					put1.add(Bytes.toBytes("userid"), Bytes.toBytes(UserID),
							Bytes.toBytes(""));

					// table1.put(put1);

					context.write(
							new ImmutableBytesWritable(Bytes.toBytes("retweet")),
							put1);

				}

				// parse place

				JSONObject place = (JSONObject) jsonObject.get("place");

				String name = "NA";

				if (place != null) {

					// this is for q5
					name = (String) place.get("name");

					name = name.replaceAll("[^a-zA-Z]", "");
					// put the place time stamp for q5
					Put put1 = new Put(Bytes.add(Bytes.toBytes(name + ":"),
							Bytes.toBytes(convertdatformat(created_at))));
					put1.add(Bytes.toBytes("tweetid"),
							Bytes.toBytes("tweetid"), Bytes.toBytes(TweetID));

					// table1.put(put1);

					context.write(
							new ImmutableBytesWritable(Bytes.toBytes("q5")),
							put1);

				}

				// for q2
				Put put1 = new Put(Bytes.add(Bytes.toBytes(UserID + ":"),
						Bytes.toBytes(convertdatformat(created_at))));
				put1.add(Bytes.toBytes("tweetid"), Bytes.toBytes("tweetid"),
						Bytes.toBytes(TweetID));
				put1.add(Bytes.toBytes("tweetid"), Bytes.toBytes("text"),
						Bytes.toBytes(Tweet));

				context.write(new ImmutableBytesWritable(Bytes.toBytes("all")),
						put1);

				// for q6

				/*
				 * Put put = new Put(Bytes.toBytes(UserID));
				 * 
				 * put.add(Bytes.toBytes("t"), Bytes.toBytes(TweetID),
				 * Bytes.toBytes(""));
				 * 
				 * 
				 * context.write(new
				 * ImmutableBytesWritable(Bytes.toBytes("q6")), put);
				 */

				// System.out.println("aa");

			} catch (org.json.simple.parser.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private static String convertdatformat(String d)
				throws java.text.ParseException {
			return parseTime(d);

		}

		public static String parseTime(String oldForm) {
			String[] createdTime = oldForm.split(" ");
			String month = createdTime[1];
			if (createdTime[1].equalsIgnoreCase("Jan")) {
				return createdTime[5] + "-01-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Feb")) {
				return createdTime[5] + "-02-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Mar")) {
				return createdTime[5] + "-03-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Apr")) {
				return createdTime[5] + "-04-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("May")) {
				return createdTime[5] + "-05-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Jun")) {
				return createdTime[5] + "-06-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Jul")) {
				return createdTime[5] + "-07-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Aug")) {
				return createdTime[5] + "-08-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Sept")) {
				return createdTime[5] + "-09-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Oct")) {
				return createdTime[5] + "-10-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Nov")) {
				return createdTime[5] + "-11-" + createdTime[2] + " "
						+ createdTime[3];
			} else if (month.equalsIgnoreCase("Dec")) {
				return createdTime[5] + "-12-" + createdTime[2] + " "
						+ createdTime[3];
			}
			return null;

		}
	}

	/**
	 * @param time
	 * @return converted time in proper format
	 * @throws ParseException
	 */
	public static String convertTime(String time) throws ParseException {
		String[] sp = time.split(" ");
		String var = sp[1];

		Date date = new SimpleDateFormat("MMM", Locale.ENGLISH).parse(var);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		int month = cal.get(Calendar.MONTH) + 1;
		if (month < 10) {
			return sp[5] + "-0" + month + "-" + sp[2] + ":" + sp[3];
		} else {
			return sp[5] + "-" + month + "-" + sp[2] + ":" + sp[3];
		}
	}
}