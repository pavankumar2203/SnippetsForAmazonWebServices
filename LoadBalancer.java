/**
 * @author Pavan kumar Sunder
 * @email psunder@andrew.cmu.edu
 * 
 * This uses the load balancer to add more ec2 instances and spread the load
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusRequest;
import com.amazonaws.services.ec2.model.DescribeInstanceStatusResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStatus;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.RegisterInstancesWithLoadBalancerResult;

public class LoadBalancer {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws IOException {

		Properties properties = new Properties();
		properties.load(CreateInstance.class
				.getResourceAsStream("/AwsCredentials.properties"));

		BasicAWSCredentials credentials = new BasicAWSCredentials(
				properties.getProperty("accessKey"),
				properties.getProperty("secretKey"));

		// Create an ELB and configures the appropriate port forwarding and
		// health checks programmatically.
		System.out
				.println("Create an ELB and configures the appropriate port forwarding and health checks programmatically.");
		AmazonElasticLoadBalancingClient elb = new AmazonElasticLoadBalancingClient(
				credentials);

		// create load balancer
		CreateLoadBalancerRequest lbRequest = new CreateLoadBalancerRequest();
		lbRequest.setLoadBalancerName("loader");
		List<Listener> listeners = new ArrayList<Listener>(1);
		listeners.add(new Listener("HTTP", 80, 80));
		listeners.add(new Listener("HTTP", 8080, 8080));
		// listeners.add(new Listener("SSH", 22,22));

		lbRequest.withAvailabilityZones("us-east-1a");
		lbRequest.setListeners(listeners);

		CreateLoadBalancerResult lbResult = elb.createLoadBalancer(lbRequest);
		System.out.println("created load balancer loader "
				+ lbResult.getDNSName());

		String elbdns = lbResult.getDNSName();

		// wait until the service is up and running
		System.out.println("wait 3 mins for service to run");
		try {
			Thread.sleep(1000 * 60 * 1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Launch an m1.small EC2 instance with AMI ID ami-69e3d500 in the same
		// availability zone as the ELB

		String requestspersecond = "";

		do {

			System.out
					.println("Launch an m1.small EC2 instance with AMI ID ami-69e3d500 in the same availability zone as the ELB");
			// Create an Amazon EC2 Client
			AmazonEC2Client ec2 = new AmazonEC2Client(credentials);

			// Create Instance Request
			RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

			// Configure Instance Request
			runInstancesRequest.withImageId("ami-69e3d500")
					.withInstanceType("m1.small").withMinCount(1)
					.withMaxCount(1).withKeyName("pa1") // this is the key pair
														// generated
					.withSecurityGroupIds("sg-8ad619ef");

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

			}

			// Attach the launched instance to the ELB.
			System.out.println("Attach the launched instance to the ELB.");
			// get the running instances
			DescribeInstancesResult describeInstancesRequest = ec2
					.describeInstances();
			List<Reservation> reservations = describeInstancesRequest
					.getReservations();
			List<Instance> instances = new ArrayList<Instance>();

			for (Reservation reservation : reservations) {

				instances.addAll(reservation.getInstances());

			}

			// get instance id's
			String id;
			List instanceId = new ArrayList();
			List instanceIdString = new ArrayList();
			Iterator<Instance> iterator = instances.iterator();
			while (iterator.hasNext()) {
				String ins = iterator.next().getInstanceId();

				System.out.println("is it me ? " + ins);
				if (ins.equals(instance_id)) {

					System.out.println("Yes it is me " + ins);
					id = ins;
					instanceId
							.add(new com.amazonaws.services.elasticloadbalancing.model.Instance(
									id));
					instanceIdString.add(id);
				}
			}

			// register the instances to the balancer
			RegisterInstancesWithLoadBalancerRequest register = new RegisterInstancesWithLoadBalancerRequest();
			register.setLoadBalancerName("loader");
			register.setInstances((Collection<com.amazonaws.services.elasticloadbalancing.model.Instance>) instanceId);
			RegisterInstancesWithLoadBalancerResult registerWithLoadBalancerResult = elb
					.registerInstancesWithLoadBalancer(register);

			// wait after attaching
			System.out.println("wait 3 mins after ataching");
			try {
				Thread.sleep(1000 * 60 * 3);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			System.out.println("out of service to in service waiting");
			// Run the apache_benchmark from the load testing instance to the
			// ELB.
			// The benchmark should be run using 100,000 requests with 100
			// running
			// concurrently using the sample.jpg image in the same directory.

			System.out.println("Run the apache_benchmark");
			// get private address since public DNS is difficult to get
			// consistently
			String pubname = instance.getPrivateIpAddress();

			System.out.println(pubname + " is my name");
			System.out
					.println("My status is running now, I m enabling detailed monitoring");

			System.out.println(" detailed monitoring : "
					+ runInstancesRequest.isMonitoring());

			// run benchmark script
			String getnameDNS = "";
			// get name
			// DescribeInstancesResult describeInstancesRequest = ec2
			// .describeInstances();
			// List<Reservation> reservations = describeInstancesRequest
			// .getReservations();

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

				String lscmd = "./apache_bench.sh sample.jpg 100000 100 "
						+ elbdns + " logfile &>> results.txt";

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

					if (line.contains("Requests per second")) {
						requestspersecond = line.split(":")[1].trim()
								.split(" ")[0];

						System.out.println("requestspersecond is "
								+ requestspersecond);
					}
				}

				input.close();

				OutputStream outputStream = p.getOutputStream();
				PrintStream printStream = new PrintStream(outputStream);

				// String resultstxt = printStream.toString();

				// System.out.println("i am writing " + resultstxt);

				printStream.println();
				printStream.flush();
				printStream.close();

			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e2) {
				System.out.println("Pblm found2.");
			}

			// run cloud watch now to get CPU Utilization

			System.out.println("ran benchmark");

			// Retrieve and parse the result of the Apache Benchmark and
			// retrieve
			// the Requests/second metric. Save this value in a file for future
			// reference.
			System.out
					.println("Retrieve and parse the result of the Apache Benchmark and retrieve the Requests/second metric");

			// If the number of requests per second is less than 2000, add
			// another
			// instance and benchmark it by repeating steps 2-6.
			System.out
					.println("If the number of requests per second is less than 2000, add another instance and benchmark it by repeating steps 2-6.");

		} while ((int) (Float.parseFloat(requestspersecond)) < 2000);
	}

}
