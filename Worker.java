

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**Class that does the remote worker.
 * 
 * @author Tiago Pais
 *
 */
public class Worker {

	/**
	 * @param args[0] idle time in milliseconds
	 */
	public static void main(String[] args) {
		//threshold (after that if its idle it stops)
		final int IDLET=Integer.parseInt(args[0]);
		// wait time between calls to SQS (sec)
		final int SQS_CALL_INTERVAL=20;

		//initial idle time
		long idle=System.currentTimeMillis();
		
		//start SQS
		AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usEast = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast);
		
		//get tasks and result Queue URLs
		String tasksUrl=sqs.getQueueUrl(new GetQueueUrlRequest("tasks")).getQueueUrl();
		String resultUrl=sqs.getQueueUrl(new GetQueueUrlRequest("result")).getQueueUrl();

		String [] message;
		//by default is not successful
		int result=1;
		//while it hasn't been idle for to many time
		while((System.currentTimeMillis()-idle)<IDLET){
			try{
				//get message from SQS
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(tasksUrl);
				//if queue is empty waits 20s for a new message
				//only returns one message each time
				receiveMessageRequest.withWaitTimeSeconds(SQS_CALL_INTERVAL).setMaxNumberOfMessages(1);
				List <Message> messages=sqs.receiveMessage(receiveMessageRequest).getMessages();

				//if we have messages
				if(!messages.isEmpty()){
					//it has a visibility timeout of 30s that guarantees that no other worker accesses before deleting it.
					sqs.deleteMessage(new DeleteMessageRequest(tasksUrl, messages.get(0).getReceiptHandle()));
					//parse message (id and time)
					message=messages.get(0).getBody().split(":");	
					//run task
					try{
						Thread.sleep(Integer.parseInt(message[1]));
						//if it succeeds
						result=0;
					}catch(InterruptedException e){
						//if it doesn't
						result=1;
					}finally{
						//update result queue on SQS
						sqs.sendMessage(new SendMessageRequest(resultUrl, message[0]+":"+message[1]+":"+ result));
					}
					//update idle time
					idle=System.currentTimeMillis();
				}	
			}catch (AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which means the client encountered " +
						"a serious internal problem while trying to communicate with SQS, such as not " +
						"being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			}

		}

		//close machine
		try{
			//set up EC2
			AmazonEC2 ec2 = new AmazonEC2Client(new ClasspathPropertiesFileCredentialsProvider());
			ec2.setRegion(usEast);
			
		URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/instance-id");
		URLConnection EC2MD = EC2MetaData.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
		List<String> EC2Id = new ArrayList<String>();
		EC2Id.add(in.readLine());
		
		// Terminate instances.
	    TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(EC2Id);
	    ec2.terminateInstances(terminateRequest);
		
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
