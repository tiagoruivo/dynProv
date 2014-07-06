

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;

/**class of the front end scheduler
 * 
 * @author Tiago Pais
 *
 */
public class Scheduler {
	
	/**method that handles scheduling for local resources
	 * 
	 * @param port port to establish connection with client
	 * @param workers number of local workers in thread pool
	 */
	private static void local(int port, int workers){
		//list of tasks to perform
		List <String> tasks=new ArrayList<String>();
		//list of results to return to client (synchronized to avoid conflicts adding and removing from different threads)
		List <String> results=Collections.synchronizedList(new ArrayList<String>());
		String task;


		try{
			//establish connection
			ServerSocket ss =new ServerSocket(port);
			Socket s =ss.accept();
			DataInputStream i= new DataInputStream(s.getInputStream());
			while(true){	
				//get tasks
				task=i.readUTF();				
				//if it's last task end loop
				if(task.equals("END")){
					break;
					//if it's not add to in memory queue	
				}else{
					tasks.add(task);	
				}
			}
			//close connection
			s.close();

			//launch thread to return results
			Thread thread= new Thread(new ResultsLocal(ss,tasks.size(),results));
			thread.start();

			//create fixed pool of threads
			ExecutorService executor = Executors.newFixedThreadPool(workers);
			//for each task start running it when thread in pool available
			for (String t : tasks) {
				Runnable worker = new LocalWorker(t,results);
				executor.execute(worker);
			}
			//close pool
			executor.shutdown();
			//wait for the threads to finish
			while (!executor.isTerminated()) {
			}

			//wait for the results thread to finish
			thread.join();

		}catch(Exception e){
			e.printStackTrace();
		}


	}
	
	/** Method that handles scheduling for remote resources
	 * 
	 * @param port port for the socket connection
	 */
	private static void remote(int port){
		try{
			//establish connection
			ServerSocket ss=new ServerSocket(port);
			Socket s =ss.accept();
			DataInputStream i= new DataInputStream(s.getInputStream());
			//list of tasks to send at once to make it more efficient
			List <SendMessageBatchRequestEntry> tasksBatch=new ArrayList<SendMessageBatchRequestEntry>();
			String task;
			int numTask=0;

			//start SQS
			AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
			Region usEast = Region.getRegion(Regions.US_EAST_1);
			sqs.setRegion(usEast);
			
			//list queues and delete them to avoid old messages
			// List queues
		
			for (String queueUrl : sqs.listQueues().getQueueUrls()) {
    			sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
			}

			//create queues for the results and the tasks and get their urls
			CreateQueueRequest createQueueRequest = new CreateQueueRequest("tasks");
			String tasksUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			CreateQueueRequest createQueueRequest2 = new CreateQueueRequest("result");
			String resultUrl = sqs.createQueue(createQueueRequest2).getQueueUrl();
			
			while(true){	
				//get tasks
				task=i.readUTF();				
				//if it's last task
				if(task.equals("END")){
					//add last tasks if there are any
					if(!tasksBatch.isEmpty()){
						sqs.sendMessageBatch(new SendMessageBatchRequest(tasksUrl, tasksBatch));
					}
					//and end loop
					break;
				//if its a normal task
				}else{
					//add it to list
					tasksBatch.add(new SendMessageBatchRequestEntry().withMessageBody(task).withId(""+numTask));
					//update counter
					numTask++;
					//we add batches of 10 tasks to queue
					//when list has 10 tasks add the batch and empty the list
					if(tasksBatch.size()==10){
						sqs.sendMessageBatch(new SendMessageBatchRequest(tasksUrl, tasksBatch));
						tasksBatch.clear();
					}
				}

			}
			
			//launch thread to return results
			Thread resultsThread=new Thread(new ResultsRemote(ss,numTask,sqs,resultUrl));
			resultsThread.start();
			
			//handle resources
			resources(sqs,tasksUrl);
			
			//wait for results thread to finish returning results
			resultsThread.join();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	private static void resources(AmazonSQS sqs, String tasksUrl){
		int workers, oldQueue=0, queue=1;
		final int MAX_NUMBER_OF_WORKERS=32;
		
		//set up EC2
		AmazonEC2 ec2 = new AmazonEC2Client(new ClasspathPropertiesFileCredentialsProvider());
		Region usEast = Region.getRegion(Regions.US_EAST_1);
		ec2.setRegion(usEast);
		
		List <SpotInstanceRequest> instances=new ArrayList <SpotInstanceRequest>();
		
		//set up instance, it is a one time request
		RequestSpotInstancesRequest requestRequest = new RequestSpotInstancesRequest();
		requestRequest.setSpotPrice("0.15");
		requestRequest.setInstanceCount(Integer.valueOf(1));
		LaunchSpecification launchSpecification = new LaunchSpecification();
		launchSpecification.setImageId("ami-7a204313");
		launchSpecification.setInstanceType("m1.small");
		ArrayList<String> securityGroups = new ArrayList<String>();
		securityGroups.add("pa4");
		launchSpecification.setSecurityGroups(securityGroups);
		requestRequest.setLaunchSpecification(launchSpecification);
		
		//monitor queue, until queue==0 since we only put one file in tasks in this implementation
		while(queue>0){		
			try{
				//check each second
				Thread.sleep(1000);
				//check queue size
				GetQueueAttributesRequest qar = new GetQueueAttributesRequest(tasksUrl);
				qar.setAttributeNames( Arrays.asList("ApproximateNumberOfMessages"));
				queue=Integer.parseInt(sqs.getQueueAttributes(qar).getAttributes().get("ApproximateNumberOfMessages"));
				
				//if queue grows																							
				if(queue>oldQueue){
					//update oldQueue
					oldQueue=queue;
					//get number of machines
					workers=0;
					//iterate through all the instances we initiated
					for (SpotInstanceRequest req : instances){
						//get state
						String state=req.getState();
						//if its starting (open) or running (active) add to workers
						if(state.equals("open")||state.equals("active")){
							workers++;
						}else{
							//workers that were already terminated or weren't able to start are eliminated from list
							instances.remove(req);
						}
					}
					if(workers<MAX_NUMBER_OF_WORKERS){
						//request for instance	
						RequestSpotInstancesResult requestResult = ec2.requestSpotInstances(requestRequest);
						//add new machine
						instances.addAll(requestResult.getSpotInstanceRequests());
					}

				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}

	
	/**
	 * 
	 * @param args[0] port for the connection with the client
	 * @param args[1] local or remote workers
	 * @param args[2] if local workers, how many
	 */
	public static void main (String [] args){
		System.out.println("running with "+args[0]+" , "+ args[1]);
		
		//handle tasks according to resource type
		if(args[1].equals("rw")){
			remote(Integer.parseInt(args[0]));			
		} else {
			local(Integer.parseInt(args[0]),Integer.parseInt(args[2]));
		}

	}
}
