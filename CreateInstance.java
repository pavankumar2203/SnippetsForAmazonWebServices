
/**
 * @author Pavan kumar Sunder
 * @email psunder@andrew.cmu.edu
 * 
 * This compares different instances with respect to load testing
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Monitoring;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class CreateInstance {

	public static void main(String[] args) throws IOException {

		// Load the Properties File with AWS Credentials

		String[] instance_types = { "m1.small", "m1.medium", "m1.large" };

		// run the benchmark on all types of instances
		for (int j = 0; j < 3; j++) {
			Properties properties = new Properties();
			properties.load(CreateInstance.class
					.getResourceAsStream("/AwsCredentials.properties"));

			BasicAWSCredentials bawsc = new BasicAWSCredentials(
					properties.getProperty("accessKey"),
					properties.getProperty("secretKey"));

			// Create an Amazon EC2 Client
			AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);

			// Create Instance Request
			RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

			// Configure Instance Request
			runInstancesRequest.withImageId("ami-69e3d500")
					.withInstanceType(instance_types[j]).withMinCount(1)
					.withMaxCount(1).withKeyName("pa1") // this is the key pair
														// generated
					.withSecurityGroupIds("sg-8ad619ef").withMonitoring(true);

			// runInstancesRequest.setSubnetId("525a6026");
			// Launch Instance
			RunInstancesResult runInstancesResult = ec2
					.runInstances(runInstancesRequest);

			// Return the Object Reference of the Instance just Launched
			Instance instance = runInstancesResult.getReservation()
					.getInstances().get(0);

			// get the instance id
			String instance_id = instance.getInstanceId();

			System.out.println("Instance created " + instance_id + " "
					+ instance.getInstanceType());

			// get the name of the state
			System.out.println();
			System.out.println(instance.getState().getName());

			String status = "";

			// get the instance state
			DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest()
					.withInstanceIds(instance.getInstanceId());
			DescribeInstanceStatusResult describeInstanceResult = ec2
					.describeInstanceStatus(describeInstanceRequest);

			List<InstanceStatus> state = describeInstanceResult
					.getInstanceStatuses();
			while (state.size() < 1) {
				// Do nothing, just wait, have thread sleep if needed
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				describeInstanceResult = ec2
						.describeInstanceStatus(describeInstanceRequest);
				state = describeInstanceResult.getInstanceStatuses();
			}

			// now the instance is running
			status = state.get(0).getInstanceState().getName();

			System.out.println(status + " new code");

			if (status.equals("running")) {

				// wait for 2 mins it to initialize

				try {
					Thread.sleep(1000 * 60 * 2);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println(instance.getState().getName());

				// get private address since public DNS is difficult to get
				// consistently
				String pubname = instance.getPrivateIpAddress();

				System.out.println(pubname + " is my name");
				System.out
						.println("My status is running now, I m enabling detailed monitoring");

				// if instance is not monitored, monitor it
				if (instance.getMonitoring().getState().equals("disabled")) {

					Monitoring mon = new Monitoring();
					mon.setState("enabled");
					instance.setMonitoring(mon);

					// instance.withMonitoring(mon);

					System.out
							.println("Monitoring " + instance.getMonitoring());

				}

				System.out.println(" detailed monitoring : "
						+ runInstancesRequest.isMonitoring());

				// start the amazon cloud watch
				AmazonCloudWatch cloudService = new AmazonCloudWatchClient(
						bawsc);

				GetMetricStatisticsRequest getMetricStatisticsRequest = new GetMetricStatisticsRequest();
				getMetricStatisticsRequest.setMetricName("CPUUtilization");
				Date s = new Date();
				getMetricStatisticsRequest.setPeriod(60 * 1); // statistics six
				// minute //No I18N
				ArrayList<String> stats = new ArrayList<String>();
				stats.add("Average");
				getMetricStatisticsRequest.setStatistics(stats);
				getMetricStatisticsRequest.setNamespace("AWS/EC2");

				getMetricStatisticsRequest.setStartTime(s);

				// run benchmark script
				String getnameDNS = "";
				// get name
				DescribeInstancesResult describeInstancesRequest = ec2
						.describeInstances();
				List<Reservation> reservations = describeInstancesRequest
						.getReservations();

				for (Reservation reservation : reservations) {
					for (Instance instance1 : reservation.getInstances()) {
						if (instance1.getInstanceId().equals(
								instance1.getInstanceId()))
							getnameDNS = instance1.getPublicDnsName();
					}
				}

				getnameDNS = instance.getPrivateIpAddress();

				System.out.println("benchmarking " + getnameDNS);
				try {

					for (int i = 0; i < 10; i++) {
						String lscmd = "./apache_bench.sh sample.jpg 100000 100 "
								+ getnameDNS + " logfile";

						System.out.println(lscmd);
						Process p = Runtime.getRuntime().exec(lscmd);
						p.waitFor();

						String line = "";

						BufferedReader error = new BufferedReader(
								new InputStreamReader(p.getErrorStream()));
						while ((line = error.readLine()) != null) {
							System.out.println(line);
						}
						error.close();

						BufferedReader input = new BufferedReader(
								new InputStreamReader(p.getInputStream()));
						while ((line = input.readLine()) != null) {
							System.out.println(line);
						}

						input.close();

						OutputStream outputStream = p.getOutputStream();
						PrintStream printStream = new PrintStream(outputStream);
						printStream.println();
						printStream.flush();
						printStream.close();

					}
				} catch (IOException e1) {
					e1.printStackTrace();
				} catch (InterruptedException e2) {
					System.out.println("Pblm found2.");
				}

				// run cloud watch now to get CPU Utilization

				System.out.println("running cloud watch");

				Date e = new Date();
				getMetricStatisticsRequest.setEndTime(e);
				System.out.println("request for instanceId ::"
						+ instance.getInstanceId() + "\n request : "
						+ getMetricStatisticsRequest);
				GetMetricStatisticsResult getMetricStatisticsResult = cloudService
						.getMetricStatistics(getMetricStatisticsRequest);
				java.util.List<Datapoint> datapointsList = getMetricStatisticsResult
						.getDatapoints();
				System.out.println("AmzonCloudWatcher.main() datapointsList : "
						+ datapointsList);
				System.out
						.println("################################################");

				System.out.println(cloudService
						.getMetricStatistics(getMetricStatisticsRequest));
				System.out.println("finished.");

				ArrayList<String> instanceIds = new ArrayList<String>();

				// terminate them
				instanceIds.add(instance.getInstanceId());

				TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(
						instanceIds);
				ec2.terminateInstances(terminateRequest);

			}

		}

	}

}
