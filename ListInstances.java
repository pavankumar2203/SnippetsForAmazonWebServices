import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Monitoring;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class ListInstances {

	public static void main(String[] args) throws IOException {
		// Load the Properties File with AWS Credentials
		Properties properties = new Properties();
		properties.load(ListInstances.class
				.getResourceAsStream("/AwsCredentials.properties"));

		BasicAWSCredentials bawsc = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));

		// Launch an EC2 Client
		AmazonEC2Client amazonEC2Client = new AmazonEC2Client(bawsc);

		// Create Instance Request
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		
		// Configure Instance Request
		runInstancesRequest.withImageId("ami-69e3d500")
				.withInstanceType("t1.micro").withMinCount(1).withMaxCount(1)
				.withKeyName("project1.1") // this is the key pair generated
				.withSecurityGroups("default");

		// Launch Instance
		RunInstancesResult runInstancesResult = amazonEC2Client
				.runInstances(runInstancesRequest);

		// Return the Object Reference of the Instance just Launched
		Instance instance1 = runInstancesResult.getReservation().getInstances()
				.get(0);

		String instance_id = instance1.getInstanceId();

		System.out.println("Instance created " + instance_id + " "
				+ instance1.getInstanceType());

		// Obtain a list of Reservations
		List<Reservation> reservations = amazonEC2Client.describeInstances()
				.getReservations();

		int reservationCount = reservations.size();

		ArrayList<String> instanceIds = new ArrayList<String>();

		for (int i = 0; i < reservationCount; i++) {
			List<Instance> instances = reservations.get(i).getInstances();

			int instanceCount = instances.size();

			// Print the instance IDs of every instance in the reservation.
			for (int j = 0; j < instanceCount; j++) {
				Instance instance = instances.get(j);

				String status = "";
				
			
				
				if (instance.getState().getName().equals("pending")) {

					try {
						
						DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest().withInstanceIds(instance.getInstanceId());
						DescribeInstanceStatusResult describeInstanceResult = amazonEC2Client.describeInstanceStatus(describeInstanceRequest);
						
						List<InstanceStatus> state = describeInstanceResult.getInstanceStatuses();
						while (state.size() < 1) { 
						    // Do nothing, just wait, have thread sleep if needed
							Thread.sleep(4000);
						    describeInstanceResult = amazonEC2Client.describeInstanceStatus(describeInstanceRequest);
						    state = describeInstanceResult.getInstanceStatuses();
						}
						status = state.get(0).getInstanceState().getName();
						
						System.out.println(status + " new code");
						
						/*
						while (instance.getState().getName().equals("pending")) {
							Thread.sleep(4000);
							System.out.println("sleeping "+ instance.getState().getName() + " " + instance.getMonitoring().getState());
							
							if (instance.getState().getName().equals("running"))
								break;
						}
						
						System.out.println("NOw wake up " + instance.getState().getName());

						*/
						
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}


				

				if (status.equals("running")) {
					System.out.println(instance.getInstanceId());

					instanceIds.add(instance.getInstanceId());

					// change the monitoring state

					System.out
							.println("Monitoring " + instance.getMonitoring());

					if (instance.getMonitoring().getState().equals("disabled")) {
						Monitoring mon = new Monitoring();
						mon.setState("enabled");
						instance.setMonitoring(mon);

						System.out.println("Monitoring "
								+ instance.getMonitoring());

					}

					// run benchmark script

					String[] env = { "PATH=/bin:/usr/bin/" };
					// String cmd = "you complete shell command"; //e.g test.sh
					// -dparam1 -oout.txt
					// Process process = Runtime.getRuntime().exec(cmd, env);

					String cmd = "ls -l";

					String getnameDNS = instance.getPublicDnsName();
					try {
						String lscmd = "./apache_bench.sh sample.jpg 100000 100 "+getnameDNS+" logfile";
						Process p = Runtime.getRuntime().exec(lscmd);
						p.waitFor();
						BufferedReader reader = new BufferedReader(
								new InputStreamReader(p.getInputStream()));
						String line = reader.readLine();
						while (line != null) {
							System.out.println(line);
							line = reader.readLine();
						}
					} catch (IOException e1) {
						e1.printStackTrace();
					} catch (InterruptedException e2) {
						System.out.println("Pblm found2.");
					}

					System.out.println("finished.");

					/*
					 * try { Process proc = Runtime.getRuntime().exec(cmd, env);
					 * BufferedReader read = new BufferedReader( new
					 * InputStreamReader(proc.getInputStream())); try {
					 * proc.waitFor(); } catch (InterruptedException e) {
					 * System.out.println(e.getMessage()); } while
					 * (read.ready()) { System.out.println(read.readLine()); } }
					 * catch (IOException e) {
					 * System.out.println(e.getMessage()); }
					 */

				}

			}
		}

		// terminate them

		// TerminateInstancesRequest terminateRequest = new
		// TerminateInstancesRequest(instanceIds);
		// amazonEC2Client.terminateInstances(terminateRequest);

	}

}
