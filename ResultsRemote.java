import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
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


/** Class for the thread that returns result to client if we use remote workers
 * 
 * @author Tiago Pais
 */
public class ResultsRemote implements Runnable{
	private int numTasks,completed=0;
	private ServerSocket ss;
	private AmazonSQS sqs;
	private String resultUrl;
	
	/**Constructor
	 * 
	 * @param ss Server socket to accept requests
	 * @param numTasks total number of tasks to perform
	 * @param sqs object for SQS to access queues 
	 * @param resultUrl url of the results queue
	 */
	public ResultsRemote(ServerSocket ss, int numTasks, AmazonSQS sqs, String resultUrl){
		this.numTasks=numTasks;
		this.ss=ss;
		this.sqs=sqs;
		this.resultUrl=resultUrl;
	}
	
	/**Method that runs thread
	 * 
	 */
	public void run(){
		//until returning the result for all the tasks
		while(completed<numTasks){
			try{
				//accept connection from client
				Socket s =ss.accept();
				DataOutputStream o= new DataOutputStream(s.getOutputStream());
				//messages come in group of 10 from queue
				List <Message> messages;
				//if list has less than 10 we have reached the end of the results
				do{
					//get 10 results from queue
					ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(resultUrl);
					receiveMessageRequest.setMaxNumberOfMessages(10);
					messages=sqs.receiveMessage(receiveMessageRequest).getMessages();
					//iterate list
					for (Message message : messages) {
						//delete message from queue
						sqs.deleteMessage(new DeleteMessageRequest(resultUrl, message.getReceiptHandle()));
						//send result to client
						o.writeUTF(message.getBody());
						//update completed tasks
						completed++;
					}						
				}while(messages.size()>9);
				//let client know that there are no more results
				o.writeUTF("END");
				//close connection
				s.close();
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}