import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;


public class customcloudmetric {
	

		public static void main(String[] args) throws IOException {
			
			
			

			Properties properties = new Properties();
			properties.load(CreateInstance.class
					.getResourceAsStream("/AwsCredentials.properties"));

			BasicAWSCredentials credentials = new BasicAWSCredentials(
					properties.getProperty("accessKey"),
					properties.getProperty("secretKey"));


			
			
			
			System.out.println("Create CloudWatch Alarms");
			AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient(
					credentials);

			// scale up
			//String upArn = arn; // from the policy request

			// Scale Up
			PutMetricAlarmRequest upRequest = new PutMetricAlarmRequest();
			upRequest.setAlarmName("AlarmName-up");
			upRequest.setMetricName("CPUUtilization");

			List dimensions = new ArrayList();
			Dimension dimension = new Dimension();
			dimension.setName("ASGName");
			dimension.setValue("ASGName");
			upRequest.setDimensions(dimensions);

			upRequest.setNamespace("AWS/EC2");
			upRequest
					.setComparisonOperator(ComparisonOperator.GreaterThanThreshold);
			upRequest.setStatistic(Statistic.Average);
			upRequest.setUnit(StandardUnit.Percent);
			upRequest.setThreshold(80d);
			upRequest.setPeriod(300);
			upRequest.setEvaluationPeriods(1);

			List actions = new ArrayList();
			//actions.add(upArn); // This is the value returned by the ScalingPolicy
								// request
			upRequest.setAlarmActions(actions);

			cloudWatchClient.putMetricAlarm(upRequest);

			System.out.println("Added scale up to cloud watch");

			
			
			
		}


}
