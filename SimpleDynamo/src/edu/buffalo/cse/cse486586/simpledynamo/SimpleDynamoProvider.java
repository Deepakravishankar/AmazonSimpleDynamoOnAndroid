package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;

@SuppressLint("UseSparseArrays")
public class SimpleDynamoProvider extends ContentProvider {
	HashMap <String,String>nodePosition=new HashMap<String,String>();
	HashMap<Integer,String> mapNodes=new HashMap<Integer,String>();
	ArrayList<String> Hash=new ArrayList<String>();
	MatrixCursor localCursor=new MatrixCursor(new String [] {KEY_FIELD,VALUE_FIELD});
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static String portStr=null;
	static String myPort=null;
	static String key_Received="";
	static String val_Received="";
	static final int SERVER_PORT=10000;
	static volatile int queryIteration=0;
	static volatile int queryResponseIteration=0;
	static volatile int querySyncIteration=0;
	static volatile boolean loop=true;
	static volatile boolean syncFlag=false;

	//This method will be called first when a node is started.
	@Override
	public boolean onCreate() {

		//Each node would have details about the next two nodes for replication
		nodePosition.put("5554","5558_5560");
		nodePosition.put("5558","5560_5562");
		nodePosition.put("5560","5562_5556");
		nodePosition.put("5562","5556_5554");
		nodePosition.put("5556","5554_5558");

		//Reverse Map each node in the ring
		mapNodes.put(0,"5562");
		mapNodes.put(1,"5556");
		mapNodes.put(2,"5554");
		mapNodes.put(3,"5558");
		mapNodes.put(4,"5560");

		//Add the hash of all nodes in an array list
		try {
			Hash.add(genHash("5562"));
			Hash.add(genHash("5556"));
			Hash.add(genHash("5554"));
			Hash.add(genHash("5558"));
			Hash.add(genHash("5560"));
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		}

		//Get the details of the current node and port
		TelephonyManager tel = (TelephonyManager) this.getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));


		//Start a Server thread that will listen to all incoming messages
		Thread server=new Thread(new Server());
		server.start();

		//Code to differentiate if node is joining for the first time or it is recovering.
		Context context=this.getContext();
		String list[]=context.fileList();

		//If node is joining for the first time.
		if(list.length==0){
			writeToFile(portStr,myPort);
		}
		else{
			syncFlag=true;
			
			//Intimate other nodes that a given node is recovering
			String msg="join_"+portStr;
			for(int i=11108;i<=11124;i=i+4){
				Thread client=new Thread(new Client(msg,i));
				client.start();
			}
		}
		return true;
	}

	//Method that will insert values into a node.
	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		if(syncFlag){
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String key=null;
		String value=null;
		String keyId=null;
		String msg=null;
		int port;
		key=values.getAsString(KEY_FIELD);
		value=values.getAsString(VALUE_FIELD);
		//Hash the key
		try {
			keyId=genHash(key);

			//Find in which node should the key get inserted
			String node=findNodeForKey(keyId);

			//Check if the key should be inserted in the current node itself
			if(portStr.equals(node)){
				//Insert the key locally
				writeToFile(key,value+"$"+node);
			}
			else{
				//Spawn a thread to insert in the appropriate node
				msg="insert_"+key+"_"+value+"$"+node;
				port=Integer.parseInt(node)*2;
				Thread client3=new Thread(new Client(msg,port));
				client3.start();
			}

			//Spawn two client threads and send them for replication
			String replicas[]=nodePosition.get(node).split("_");
			msg="insert_"+key+"_"+value+"$"+node;

			//First thread
			port=Integer.parseInt(replicas[0])*2;
			Thread client4=new Thread(new Client(msg,port));
			client4.start();

			//Second thread
			port=Integer.parseInt(replicas[1])*2;
			Thread client5=new Thread(new Client(msg,port));
			client5.start();

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} 
		return null;
	}

	//Function to find the in which node should the given key be inserted.
	String findNodeForKey(String keyId){
		int current=0;
		int prev;
		//Cover the corner cases first.
		if(keyId.compareTo(Hash.get(0)) <= 0 || keyId.compareTo(Hash.get(4)) > 0 ){
			current=0;
		}
		else
		{
			//Get where a key will get inserted.
			for(int i=1;i<Hash.size();i++){
				prev=i-1;
				if((keyId.compareTo(Hash.get(prev)) > 0) && (keyId.compareTo(Hash.get(i)) <=0)){
					current=i;
					break;
				}
			}
		}
		//Return the node that is the coordinator for a key.
		return mapNodes.get(current);
	}

	//Function to write files to disk.
	public void writeToFile(String key, String value){
		Context context=this.getContext();
		try {
			FileOutputStream fos=context.getApplicationContext().openFileOutput(key,Context.MODE_PRIVATE);
			fos.write(value.getBytes());
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Function that will query from a given node.
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		if(syncFlag){
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String msg;
		String ret=String.valueOf(Integer.parseInt(portStr)*2);
		String FILENAME=selection;
		String VALUE;
		Context context=this.getContext();
		String list[]=context.fileList();
		MatrixCursor mCursor=new MatrixCursor(new String [] {KEY_FIELD,VALUE_FIELD});

		//Get all files in the local node.
		if(FILENAME.equals("@")){
			for(int i=0;i<list.length;i++){
				if(!list[i].equals(portStr)){
					VALUE=localQuery(list[i]);
					String valueSplitter[]=VALUE.split("\\$");
					VALUE=valueSplitter[0];
					mCursor.addRow(new String[]{list[i],VALUE});
				}
			}
			return mCursor;
		}
		else if(FILENAME.equals("*")){
			//Get all files in the entire DHT
			for(int i=0;i<list.length;i++){
				if(!list[i].equals(portStr)){
					VALUE=localQuery(list[i]);
					String valueSplitter[]=VALUE.split("\\$");
					VALUE=valueSplitter[0];
					localCursor.addRow(new String[]{list[i],VALUE});
				}
			}
			//Send a request to all the other nodes for their keys
			for(int i=11108;i<=11124;i=i+4){
				msg="queryAll_"+ret;
				Thread client=new Thread(new Client(msg,i));
				client.start();
			}
			while(loop==true){

			}
			loop=true;
			return localCursor;
		}
		//Query for a given key
		else{
			//Find which node should be queried
			try {
				String keyId=genHash(FILENAME);

				//Find where the given key resides and query that node.
				String node=findNodeForKey(keyId);
				if(portStr.equals(node)){
					VALUE=localQuery(FILENAME);
					String valueSplitter[]=VALUE.split("\\$");
					VALUE=valueSplitter[0];
					mCursor.addRow(new String[]{FILENAME,VALUE});
					return mCursor;
				}
				else{
					//Query coordinator node.
					msg="query_"+FILENAME+"_"+ret+"_"+String.valueOf(queryResponseIteration);
					int port=Integer.parseInt(node)*2;
					Thread client1=new Thread(new Client(msg,port));
					client1.start();

					//Query First Replica.
					String replicas[]=nodePosition.get(node).split("_");
					port=Integer.parseInt(replicas[0])*2;
					Thread client2=new Thread(new Client(msg,port));
					client2.start();

					//Wait for the response from the other node.
					while(loop == true){

					}
					loop=true;
					mCursor.addRow(new String[]{key_Received,val_Received});
					return mCursor;
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	//Function to query for a single file in the node.
	public String localQuery(String key){
		Context context=this.getContext();
		String value=null;
		try {
			FileInputStream fis = context.getApplicationContext().openFileInput(key);      
			BufferedReader br = new BufferedReader(new InputStreamReader(fis)); 
			value=br.readLine();
			br.close();                                                         
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return value;
	}

	//Function to query for all the files in the node.
	public String localQueryAll(){
		StringBuffer response=new StringBuffer();
		StringBuffer finalReply=new StringBuffer();
		StringBuffer send=new StringBuffer();
		Context context=this.getContext();
		String list[]=context.fileList();
		String val;
		int count=0;
		if(list.length >= 1){
			//Get all files in the local node
			for(int i=0;i<list.length;i++){
				if(!list[i].equals(portStr)){
					val=localQuery(list[i]);
					String valueSplitter[]=val.split("\\$");
					val=valueSplitter[0];
					response.append(list[i]+"_"+val+"_");
					count++;
				}
			}
		}

		//String object containing all the key value pairs
		finalReply.append(String.valueOf(count)+"_"+response);
		send.append("queryResponseAll_"+finalReply.toString());
		return send.toString();
	}

	//Function to delete a value in a node.
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String FILENAME=selection;
		Context context=this.getContext();
		String list[]=context.fileList();
		String ret=String.valueOf(Integer.parseInt(portStr)*2);
		String msg;

		//Delete all the files in the local node.
		if(FILENAME.equals("@")){
			for(int i=0;i<list.length;i++){
				context.deleteFile(list[i]);
			}
		}
		else if(FILENAME.equals("*")){
			//Delete all the files in the local node.
			for(int i=0;i<list.length;i++){
				context.deleteFile(list[i]);
			}

			//Send message to other nodes in the DHT to delete all their nodes.
			for(int i=11108;i<=11124;i=i+4){
				msg="deleteAll_"+ret;
				Thread client=new Thread(new Client(msg,i));
				client.start();
			}
		}
		//Handle delete of a single key.
		else{
			try {
				String keyId=genHash(FILENAME);
				String node=findNodeForKey(keyId);

				//Key will be residing in 5562 in both the cases.
				if(portStr.equals(node)){
					context.deleteFile(FILENAME);
				}
				else{
					msg="delete_"+FILENAME;
					int port=Integer.parseInt(node)*2;
					Thread client=new Thread(new Client(msg,port));
					client.start();
				}

				//Send message to delete the replicas also.
				String replicas[]=nodePosition.get(node).split("_");
				msg="delete_"+FILENAME;

				//Delete first replica.
				int port=Integer.parseInt(replicas[0])*2;
				Thread client3=new Thread(new Client(msg,port));
				client3.start();

				//Delete Second replica.
				port=Integer.parseInt(replicas[1])*2;
				Thread client4=new Thread(new Client(msg,port));
				client4.start();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

	//Function to delete all the files in a node.
	public void localDeleteAll(){
		Context context=this.getContext();
		String list[]=context.fileList();
		for(int i=0;i<list.length;i++){
			context.deleteFile(list[i]);
		}
	}

	//Function to delete a single file.
	public void localDelete(String file){
		Context context=this.getContext();
		context.deleteFile(file);
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	//Function to do consistent hashing.
	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	//Function to re send values to a recovering key.
	public String resendKeysToFailedNode(String ret){
		StringBuffer response=new StringBuffer();
		StringBuffer finalResponse=new StringBuffer();
		Context context=this.getContext();
		String list[]=context.fileList();
		String value;
		int count=0;
		boolean flag=false;

		//Check if the keys in this node should get replicated in the failed node.
		String replicas[]=nodePosition.get(portStr).split("_");
		if(ret.equals(replicas[0]) || ret.equals(replicas[1])){
			flag=true;
		}

		//Append keys that should be in the recovering node.
		for(int i=0;i<list.length;i++){
			if(!list[i].equals(portStr)){
				value=localQuery(list[i]);
				String values[]=value.split("\\$");
				if(values[1].equals(ret) || (flag==true && values[1].equals(portStr))){
					response.append(list[i]+"_"+value+"_");
					count++;
				}
			}
		}

		//Send back to recovering node.
		finalResponse.append("sync_"+portStr+"_"+String.valueOf(count)+"_"+response.toString());
		return finalResponse.toString();
	}

	//Server class to handle incoming messages.
	//Copied from previous assignment.
	class Server implements Runnable{
		public void run(){
			try {
				ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
				while(true){
					Socket client = serverSocket.accept();
					try{
						BufferedReader is = new BufferedReader(
								new InputStreamReader(client.getInputStream()));
						String msg = is.readLine();
						//Thread that will work with the incoming message
						Thread manage=new Thread(new Manage(msg));
						manage.start();
					}catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					client.close();
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	//Class to manage the incoming messages. 
	class Manage implements Runnable{
		String msg;
		public Manage(String message){
			msg=message;
		}
		public void run(){
			String message[]=msg.split("_");

			//Handle incoming messages that need to be inserted
			if(message[0].equals("join")){
				if(!message[1].equals(portStr)){
					//Its a node failure. Send the appropriate keys back to the node that has failed.
					//Also this node that is sending keys.
					//should get added in the ArrayList of the recovering node.
					String obj=resendKeysToFailedNode(message[1]);
					int port=Integer.parseInt(message[1])*2;
					Thread client=new Thread(new Client(obj,port));
					client.start();
				}
			}
			//Handle insert of incoming messages.
			if(message[0].equals("insert")){
				if(syncFlag){
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				writeToFile(message[1],message[2]);
			}
			//Handle incoming query to return all the keys.
			else if(message[0].equals("queryAll")){
				if(!myPort.equals(message[1])){
					String obj=localQueryAll();
					Thread client=new Thread(new Client(obj,Integer.parseInt(message[1])));
					client.start();
				}
			}
			//Handle Response for query *.
			else if(message[0].equals("queryResponseAll")){
				queryIteration++;
				int count =Integer.parseInt(message[1]);
				int i=2;

				//Add the incoming values to a cursor object
				while(count > 0){
					localCursor.addRow(new String[]{message[i],message[i+1]});
					i=i+2;
					count--;
				}
				if(queryIteration == 3){
					queryIteration=0;
					loop=false;
				}
			}
			//Handle query of a single key.
			else if(message[0].equals("query")){
				if(syncFlag){
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				String value=localQuery(message[1]);
				String valueSplitter[]=value.split("\\$");
				value=valueSplitter[0];
				String msg="queryResponse_"+message[1]+"_"+value+"_"+message[3];
				Thread client1=new Thread(new Client(msg,Integer.parseInt(message[2])));
				client1.start();
			}
			//Handle query response of a single key.
			else if(message[0].equals("queryResponse")){
				if(queryResponseIteration==Integer.parseInt(message[3])){
					queryResponseIteration++;
					key_Received=message[1];
					val_Received=message[2];
					loop=false;
				}
			}
			//Delete all messages in the DHT
			else if(message[0].equals("deleteAll")){
				if(!myPort.equals(message[1])){
					localDeleteAll();
				}
			}
			//Handle delete of a single key.
			else if(message[0].equals("delete")){
				localDelete(message[1]);
			}
			//Sync values for an recovering node.
			else if(message[0].equals("sync")){
				querySyncIteration++;
				int count =Integer.parseInt(message[2]);
				int i=3;
				//Write the incoming values to a File
				while(count > 0){
					writeToFile(message[i],message[i+1]);
					i=i+2;
					count--;
				}
				if(querySyncIteration==4){
					querySyncIteration=0;
					syncFlag=false;
				}
			}
		}
	}

	//Client class to send requests.
	//Copied from the previous assignment.
	class Client implements Runnable{
		String message;
		int port;
		public Client(String msg,int sendToPort) {
			message=msg;
			port=sendToPort;
		}
		public void run(){
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[] {
						10, 0, 2, 2 }),port);
				DataOutputStream os=new DataOutputStream(socket.getOutputStream());
				OutputStreamWriter out=new OutputStreamWriter(os);
				out.write(message);
				out.close();
				os.close();
				socket.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}