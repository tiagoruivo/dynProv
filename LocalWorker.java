import java.util.*;


/**Class for the thread that executes tasks locally
 * 
 * @author Tiago Pais
 */
public class LocalWorker implements Runnable{
	String task;
	String [] message;
	//by default we fail execution
	int res=1;
	List <String> results;
	
	/**Constructor
	 * 
	 * @param task task to execute
	 * @param results list where to add result
	 */
	public LocalWorker(String task, List <String> results){
		this.task=task;
		this.results=results;
	}
	
	/**Method that runs thread
	 * 
	 */
	public void run(){
		//parse message to split id and time to sleep
		message=task.split(":");	
		//run task
		try{
			Thread.sleep(Integer.parseInt(message[1]));
			//if we succeed
			res=0;
		}catch(InterruptedException e){
			//if we fail
			res=1;
		}finally{
			//update result queue (add result to id and time to sleep)
			results.add(task+":"+ res);
		}
	}
}