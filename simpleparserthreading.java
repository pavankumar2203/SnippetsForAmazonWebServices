import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

class RunnableDemo implements Runnable {
	private Thread t;
	private String threadName;

	RunnableDemo(String name) {
		threadName = name;
		System.out.println("Creating " + threadName);
	}

	public void run() {
		System.out.println("Running " + threadName);
		System.out.println("Thread: " + threadName + ", "
				+ threadName.substring(8, 10));
		// Let the thread sleep for a while.
		// Thread.sleep(50);

		JSONParser parser = new JSONParser();

		try {
			
			 Properties properties = new Properties();
			  properties.load(CreateInstance.class
			  .getResourceAsStream("/AwsCredentials.properties"));
			  
			  BasicAWSCredentials credentials = new BasicAWSCredentials(
			  properties.getProperty("accessKey"),
			  properties.getProperty("secretKey"));
			  
			  AmazonS3Client s3Client = new AmazonS3Client(credentials);
			  
			  S3Object object = s3Client.getObject(new GetObjectRequest(
			  "15619project-dataset-s14", "phase2/" + threadName));
			 
			//InputStream inputStream = new FileInputStream(
				//	"C:/Users/Pavan/Desktop/CloudComputingS14/finalproject/200gbdataset/"
					//		+ threadName);

			S3ObjectInputStreamWrapper wrap = new S3ObjectInputStreamWrapper(object.getObjectContent(),s3Client);
			
			BufferedReader br = new BufferedReader(new InputStreamReader(wrap));
			//object.getObjectContent()));

			//BufferedReader br = new BufferedReader(new InputStreamReader(
				//	inputStream));

			File file = new File(threadName);
			Writer writer = new OutputStreamWriter(new FileOutputStream(file));

			writer.append("TweetID");
			writer.append(',');
			writer.append("Created_at");
			writer.append(',');
			writer.append("UserID");
			writer.append(',');
			writer.append("Tweet");
			writer.append(',');
			writer.append("RetweeteduserID");
			writer.append('\n');
			String scan;

			while ((scan = br.readLine()) != null) {
				Object obj = parser.parse(scan);

				JSONObject jsonObject = (JSONObject) obj;

				String created_at = (String) jsonObject.get("created_at");
				// System.out.println(convertdatformat(created_at));

				long TweetID = (Long) jsonObject.get("id");
				// System.out.println(TweetID);

				// parse user id

				JSONObject msg = (JSONObject) jsonObject.get("user");

				long UserID = (Long) msg.get("id");
				// System.out.println(UserID);

				// String Tweet = (String) jsonObject.get("text");

				// System.out.println(Tweet);

				// get retweeted user id
				JSONObject msgretweet = (JSONObject) jsonObject
						.get("retweeted_status");

				Long retweeteduserID;

				if (msgretweet != null) {
					JSONObject retweeteduser = (JSONObject) msgretweet
							.get("user");

					retweeteduserID = (Long) retweeteduser.get("id");
					// System.out.println("RETWEETED ID " +
					// retweeteduserID);
				} else {
					retweeteduserID = Long.MIN_VALUE;
				}

				writer.append(TweetID + "");
				writer.append(',');
				writer.append(convertdatformat(created_at));
				writer.append(',');
				writer.append(UserID + "");
				writer.append(',');
				writer.append(retweeteduserID + "");
				// writer.append("\\\"" + Tweet + "\\\"");
				writer.append('\n');

			}
			br.close();
			writer.close();

			System.out.println("Thread " + threadName + " exiting.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() {
		System.out.println("Starting " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

	private String convertdatformat(String d) throws java.text.ParseException {

		/**
		 * String s = d; //2013-10-02+00:00:00
		 * 
		 * 
		 * String time = s.split(" ")[3]; String year = s.split(" ")[5]; String
		 * date = s.split(" ")[2]; String month = s.split(" ")[1];
		 * 
		 * Calendar cal = Calendar.getInstance(); cal.setTime(new
		 * SimpleDateFormat("MMM").parse(month)); int monthInt =
		 * cal.get(Calendar.MONTH) + 1;
		 * 
		 * return year+"-"+monthInt+"-"+date+"+"+time;
		 */

		return parseTime(d);

	}

	public String parseTime(String oldForm) {
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

public class simpleparserthreading {
	public static void main(String args[]) {

		for (int i = 30; i >= 0; i--) {

			String fileappend = "";

			if (i < 10)
				fileappend = "part-0000" + i;
			else
				fileappend = "part-000" + i;

			RunnableDemo R1 = new RunnableDemo(fileappend);
			R1.start();

		}
	}
}
