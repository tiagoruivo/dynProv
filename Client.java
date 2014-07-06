

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

/**class of the client
 * 
 * @author Tiago Pais
 *
 */
public class Client {

	/** client that sends tasks to scheduler and waits for results
	 * @param args[0] ip:port
	 * @param args[1] file with tasks
	 */
	public static void main(String[] args) {
		//parses ip and port
		String [] direction= args[0].split(":");
		List <String> tasks = new ArrayList<String>();
		int id=0;
		String result;
		String [] parsedResult;
		//interval to poll results from scheduler
		final int POLLINT=5*1000;
		try{
			String line;
			//open the file	with the tasks
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[1])));
			//start reading different tasks (each one on one line)
			while((line=br.readLine())!=null){
				//take 'sleep ' out of the instruction, only get the number
				line=line.replace("sleep ", "");
				//add them to list (id and time)
				tasks.add(id+":"+line);
				//update id
				id++;
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		//establish connection with scheduler
		try{
			//create connection
			Socket s= new Socket(direction[0],Integer.parseInt(direction[1]));
			DataOutputStream o= new DataOutputStream(s.getOutputStream());
			//send each task to scheduler
			for(int k=0; k<tasks.size();k++){
				o.writeUTF(tasks.get(k));
			}
			//say that there are no more tasks and end connection
			o.writeUTF("END");
			s.close();
		
			//poll for results in a given interval
			int completed=0;
			while(completed<tasks.size()){
				//wait interval
				Thread.sleep(POLLINT);
				//establish connection
				s= new Socket(direction[0],Integer.parseInt(direction[1]));
				DataInputStream i= new DataInputStream(s.getInputStream());
				
				//get the responses
				while(true){
					//read result
					result=i.readUTF();
					//if there are no more results end
					if(result.equals("END")){
						break;
					//else print result and update number of completed
					}else {
						parsedResult=result.split(":");
						System.out.println("Task "+parsedResult[0]+": sleep "+ parsedResult[1]+" - "+ parsedResult[2]);
						completed++;
					}
			
				}
				//close connection
				s.close();
			}
		}catch(Exception e){
			e.printStackTrace();
		}	
	}

}
