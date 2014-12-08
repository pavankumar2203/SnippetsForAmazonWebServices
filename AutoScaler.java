/**
 * @author Pavan kumar Sunder
 * @email psunder@andrew.cmu.edu
 * 
 * This uses the Auto Scaling to scale in and scale ec2 instances and spread the load using the load balancer
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.PutNotificationConfigurationRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.SubscribeRequest;

public class AutoScaler {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws IOException {

		String scan;
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

		lbRequest.withAvailabilityZones("us-east-1b");
		lbRequest.setListeners(listeners);

		CreateLoadBalancerResult lbResult = elb.createLoadBalancer(lbRequest);
		System.out.println("created load balancer loader "
				+ lbResult.getDNSName());

		// this is the ELB DNS name
		String elbdns = "loader";

		// wait until the service is up and running
		System.out.println("wait 1 min for service to run");
		try {
			Thread.sleep(1000 * 10 * 1);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

		// end of step a

		// create an auto scaling client
		AmazonAutoScalingClient as = new AmazonAutoScalingClient(credentials);

		// enable detailed monitoring
		InstanceMonitoring insmon = new InstanceMonitoring();
		insmon.setEnabled(true);

		// add the security group
		ArrayList<String> sec = new ArrayList<String>();
		sec.add("sg-8ad619ef");

		// add the availabilit zone
		ArrayList<String> avzone = new ArrayList<String>();
		avzone.add("us-east-1b");

		// add the elb name
		ArrayList<String> elbs = new ArrayList<String>();
		elbs.add(elbdns);

		System.out.println("Create a Launch Configuration");
		// create launch configuration request here
		CreateLaunchConfigurationRequest launchconfig = new CreateLaunchConfigurationRequest();

		launchconfig.setImageId("ami-99e2d4f0");
		launchconfig.setInstanceType("m1.small");
		launchconfig.setInstanceMonitoring(insmon);
		launchconfig.setSecurityGroups(sec);
		launchconfig.setKeyName("pa1");
		launchconfig.setLaunchConfigurationName("AutoScaleLC");

		// add the launch config to the client
		as.createLaunchConfiguration(launchconfig);

		// create the CreateAutoScalingGroupRequest first

		System.out.println("Create an Auto Scaling Group");
		CreateAutoScalingGroupRequest casgr = new CreateAutoScalingGroupRequest();
		casgr.setAutoScalingGroupName("ASGName");
		casgr.setLaunchConfigurationName("AutoScaleLC");
		casgr.setMinSize(2);
		casgr.setMaxSize(5);
		casgr.setDesiredCapacity(2);
		casgr.setAvailabilityZones(avzone);
		casgr.setLoadBalancerNames(elbs);
		casgr.setHealthCheckType("EC2");
		casgr.setHealthCheckGracePeriod(120);
		casgr.setDefaultCooldown(300);

		// add the auto scaling request to the client
		as.createAutoScalingGroup(casgr);

		System.out.println("Create the following Auto Scale Policies");

		// policie scale up
		PutScalingPolicyRequest putScalingPolicyRequest = new PutScalingPolicyRequest();
		putScalingPolicyRequest.setScalingAdjustment(1);
		putScalingPolicyRequest.setAutoScalingGroupName("ASGName");
		putScalingPolicyRequest.setPolicyName("ScaleUP");
		putScalingPolicyRequest.setAdjustmentType("ChangeInCapacity");

		PutScalingPolicyResult result = as
				.putScalingPolicy(putScalingPolicyRequest);
		String arn = result.getPolicyARN(); // You need the policy ARN in the
											// next step so make a note of it.

		System.out.println("Scale up policy " + arn);
		// policie scale down
		PutScalingPolicyRequest putScalingPolicyRequest1 = new PutScalingPolicyRequest();
		putScalingPolicyRequest1.setScalingAdjustment(-1);
		putScalingPolicyRequest1.setAutoScalingGroupName("ASGName");
		putScalingPolicyRequest1.setPolicyName("ScaleDOWN");
		putScalingPolicyRequest1.setAdjustmentType("ChangeInCapacity");

		PutScalingPolicyResult result1 = as
				.putScalingPolicy(putScalingPolicyRequest1);
		String arn1 = result1.getPolicyARN(); // You need the policy ARN in the
												// next step so make a note of
												// it.

		System.out.println("Scale Down policy " + arn1);
		// step e
		// cloud watch

		System.out.println("Create CloudWatch Alarms");
		AmazonCloudWatchClient cloudWatchClient = new AmazonCloudWatchClient(
				credentials);

		// scale up
		String upArn = arn; // from the policy request

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
		actions.add(upArn); // This is the value returned by the ScalingPolicy
							// request
		upRequest.setAlarmActions(actions);

		cloudWatchClient.putMetricAlarm(upRequest);

		System.out.println("Added scale up to cloud watch");
		// scale down
		String upArn1 = arn1; // from the policy request

		// Scale down
		PutMetricAlarmRequest upRequest1 = new PutMetricAlarmRequest();
		upRequest1.setAlarmName("AlarmName-down");
		upRequest1.setMetricName("CPUUtilization");

		List dimensions1 = new ArrayList();
		Dimension dimension1 = new Dimension();
		dimension1.setName("ASGName");
		dimension1.setValue("ASGName");
		upRequest1.setDimensions(dimensions1);

		upRequest1.setNamespace("AWS/EC2");
		upRequest1.setComparisonOperator(ComparisonOperator.LessThanThreshold);
		upRequest1.setStatistic(Statistic.Average);
		upRequest1.setUnit(StandardUnit.Percent);
		upRequest1.setThreshold(20d);
		upRequest1.setPeriod(300);
		upRequest1.setEvaluationPeriods(1);

		List actions1 = new ArrayList();
		actions1.add(upArn1); // This is the value returned by the ScalingPolicy
								// request
		upRequest1.setAlarmActions(actions1);

		cloudWatchClient.putMetricAlarm(upRequest1);

		System.out.println("Added scale down to cloud watch");

		// watch for all the requests
		ArrayList<String> allthese = new ArrayList<String>();
		allthese.add("autoscaling:EC2_INSTANCE_LAUNCH");
		allthese.add("autoscaling:EC2_INSTANCE_LAUNCH_ERROR");
		allthese.add("autoscaling:EC2_INSTANCE_TERMINATE");
		allthese.add("autoscaling:EC2_INSTANCE_TERMINATE_ERROR");
		allthese.add("autoscaling:TEST_NOTIFICATION");

		System.out
				.println("Configure your Auto Scaling Group to notify you during scale up and scale down events using the ARN you obtained while creating the SNS topic");
		// create a topic
		AmazonSNSClient sns = new AmazonSNSClient(credentials);

		// create a SNS Topic
		CreateTopicRequest createTopicRequest = new CreateTopicRequest(
				"AutoScaling");

		CreateTopicResult response = new CreateTopicResult();
		response = sns.createTopic(createTopicRequest);

		System.out.println("wait 1 min for service to run");
		try {
			Thread.sleep(1000 * 60 * 1);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

		// String topicarn = "arn:aws:sns:us-east-1:749853777474:BeforeLaunch";

		// subscribe the policies to the SNS
		String topicarn = response.getTopicArn();
		SubscribeRequest subscribeRequest = new SubscribeRequest(arn, "email",
				"psunder@andrew.cmu.edu");
		subscribeRequest.setTopicArn(topicarn);
		SubscribeRequest subscribeRequest1 = new SubscribeRequest(arn1,
				"email", "psunder@andrew.cmu.edu");
		subscribeRequest1.setTopicArn(topicarn);
		sns.subscribe(subscribeRequest);
		sns.subscribe(subscribeRequest1);

		System.out.println("wait 1 min for service to run");
		try {
			Thread.sleep(1000 * 60 * 1);
		} catch (InterruptedException e) {

			e.printStackTrace();
		}

		PutNotificationConfigurationRequest putNotificationConfigurationRequest = new PutNotificationConfigurationRequest();
		putNotificationConfigurationRequest.setAutoScalingGroupName("ASGName");
		putNotificationConfigurationRequest.setTopicARN(topicarn);
		putNotificationConfigurationRequest.setNotificationTypes(allthese);

		/*
		 * PutNotificationConfigurationRequest
		 * putNotificationConfigurationRequest1 = new
		 * PutNotificationConfigurationRequest();
		 * putNotificationConfigurationRequest1
		 * .setAutoScalingGroupName("ASGName");
		 * putNotificationConfigurationRequest1.setTopicARN("AutoScaling");
		 * putNotificationConfigurationRequest1.setNotificationTypes(allthese);
		 */

		// notify me

		System.out
				.println("Pass all the notification requests to AWS to launch the auto-scaling group.");
		as.putNotificationConfiguration(putNotificationConfigurationRequest);
		// as.putNotificationConfiguration(putNotificationConfigurationRequest1);

		// Launch the auto scaling group

	}
}
