import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class simpleparser {

	public static void main(String[] args) throws java.text.ParseException {

		JSONParser parser = new JSONParser();

		try {

			Properties properties = new Properties();
			properties.load(CreateInstance.class
					.getResourceAsStream("/AwsCredentials.properties"));

			BasicAWSCredentials credentials = new BasicAWSCredentials(
					properties.getProperty("accessKey"),
					properties.getProperty("secretKey"));

			AmazonS3Client s3Client = new AmazonS3Client(credentials);

			// run this on all files

			for (int i = 0; i < 31; i++) {
				
				String fileappend = "";
				
				if(i < 10)
					fileappend = "part-0000" + i;
				else
					fileappend = "part-000" + i;
				
				S3Object object = s3Client.getObject(new GetObjectRequest(
						"15619project-dataset-s14", "phase2/"+fileappend));

				BufferedReader br = new BufferedReader(new InputStreamReader(
						object.getObjectContent()));
				
				File file = new File(fileappend);
				Writer writer = new OutputStreamWriter(new FileOutputStream(
						file));

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

					String Tweet = (String) jsonObject.get("text");

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
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	private static String convertdatformat(String d)
			throws java.text.ParseException {

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