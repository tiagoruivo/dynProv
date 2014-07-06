import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.*;
import java.util.*;


/** Class for the thread that returns result to client if we use local workers
 * 
 * @author Tiago Pais
 */
public class ResultsLocal implements Runnable{
	private int numTasks,completed=0;
	private ServerSocket ss;
	private List <String> results;
	
	/**Constructor
	 * 
	 * @param ss Server socket to accept requests
	 * @param numTasks total number of tasks to perform
	 * @param results list where to get the results
	 */
	public ResultsLocal(ServerSocket ss, int numTasks, List <String> results){
		this.numTasks=numTasks;
		this.ss=ss;
		this.results=results;
	}
	
	/**Method that runs thread
	 * 
	 */
	public void run(){
		//until returning the result for all the tasks
		while(completed<numTasks){
			try{
				//accept connection to client
				Socket s =ss.accept();
				DataOutputStream o= new DataOutputStream(s.getOutputStream());
				//while we have results to return (synchronized list to avoid conflicts)
				while(!results.isEmpty()){
					//write them
					o.writeUTF(results.get(0));
					//remove them from list - New results are added to the end, so position 1 doesn't change
					results.remove(0);
					//update completed tasks
					completed++;
				}
				//let client know that there aren't more results
				o.writeUTF("END");
				//close connection
				s.close();
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}