package dbparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class entryClass {
	
	static ArrayList<String> tablename;
	static HashMap<String, String>tablekey = new HashMap<String,String>();

	public static void main(String[] args) throws IOException, ParseException {
		
		/// load file with all table names into arraylist
		
		File file = new File("tableList.txt");
		if(!file.exists()) {
		    file.createNewFile();
		} 
		String name;
		BufferedReader br = new BufferedReader(new FileReader("tableList.txt"));
		tablename = new ArrayList<String>();
		while((name=br.readLine())!=null)
			tablename.add(name);
		
		br.close();
		
		/// load file with table name and primary key
		
		File prim = new File("tablekeymeta.txt");
		if(!file.exists()) {
		    file.createNewFile();
		} 
		
		br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		tablekey = new HashMap<String,String>();
		while((name=br.readLine())!=null){
			String s[]=name.split(" ");
			tablekey.put(s[0],s[1]);
		}
		
		br.close();
		
		//createTable();
		//createTable();
		//deleteTable();
		listTables();
		//inserttoTable();
		//deletefromTable();
		//updatefromTable();
		listfromTable();
		
		/// load all changes to file again
		FileWriter writer = new FileWriter("tableList.txt"); 
		  writer.write("");
		  writer.close();
		  BufferedWriter brw = new BufferedWriter(new FileWriter("tablelist.txt"));
		  for(String str: tablename) {
		  brw.write(str);
		  brw.newLine();
		}
		brw.close();
		
		/// load all changes to table primary key file
		writer = new FileWriter("tablekeymeta.txt"); 
		  writer.write("");
		  writer.close();
		BufferedWriter brw2=new BufferedWriter(new FileWriter("tablekeymeta.txt"));
		
		for( Map.Entry<String, String> hm:tablekey.entrySet() )
		{
			String tn=hm.getKey();
			String kn=hm.getValue();
			brw2.write(tn+" "+kn);
			brw2.newLine();
			System.out.println(hm.getKey()+ " "+ hm.getValue());
		}
		brw2.close();

}

	private static void listfromTable() throws IOException, ParseException {
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname=sc.next();
		String name;
		ArrayList<String> list= new ArrayList<>();
		JSONParser parser = new JSONParser();
		JSONObject obj= new JSONObject();
		
		if(!tablename.contains(tname))
		{
			System.out.println("Table doesn't Exist");
		}
		else{
			
			BufferedReader br = new BufferedReader(new FileReader(tname+"_meta.txt"));
			while((name=br.readLine())!=null){
			String s[]=name.split(" ");
			list.add(s[0]);
			}
			for(int i=0;i<list.size();i++){
			System.out.print(list.get(i)+"\t\t\t\t\t");
			}
			System.out.println();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname+".json"));
			int len = content.size();
			if (content != null) { 
			   for (int i=0;i<len;i++){
				   obj= (JSONObject) content.get(i);
				   for(int j=0;j<list.size();j++){
						System.out.print(obj.get(list.get(j))+"\t\t\t");
						}
				   System.out.println();
				   
			   }
			}
			
		}
		
	}

	private static void updatefromTable() throws IOException, ParseException {
		
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname=sc.next();
		String name;
		String pkey="";
		String value;
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag=false;
		
		if(!tablename.contains(tname))
		{
			System.out.println("Table doesn't Exist");
		}
		else{
			System.out.println("Enter Primary key Value: ");
			String prim=sc.next();			
			BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while((name=br.readLine())!=null){
				String s[]=name.split(" ");
				if(s[0].equals(tname))
				{
					pkey=s[1];
					flag=true;
				}
								
			}
			JSONArray list = new JSONArray();    
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname+".json"));
			int len = content.size();
			if (content != null && flag==true) { 
			   for (int i=0;i<len;i++){
				   
			   obj=(JSONObject) content.get(i);
			   if(obj.get(pkey).equals(prim))
			   {
				   JSONObject newObj=new JSONObject();
				   newObj.put(pkey, prim);
				   BufferedReader brmeta = new BufferedReader(new FileReader(tname+"_meta.txt"));
					while((name=brmeta.readLine())!=null){
						String s[]=name.split(" ");
						if(!s[0].equals(pkey)){
						System.out.println("Enter value for "+s[0] );
						value=sc.next();
						newObj.put(s[0], value);
				   
					}
						obj=newObj;
						
			   } 
					
			}
			list.add(obj);
			FileWriter file = new FileWriter(tname+".json");
			file.write("");	
			file.write(list.toJSONString());
			file.close();
			
		}
		
	
	}
		}
	}

	private static void deletefromTable() throws IOException, ParseException {
		
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname=sc.next();
		String name;
		String pkey="";
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag=false;
		
		if(!tablename.contains(tname))
		{
			System.out.println("Table doesn't Exist");
		}
		else{
			System.out.println("Enter Primary key Value: ");
			String prim=sc.next();			
			BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while((name=br.readLine())!=null){
				String s[]=name.split(" ");
				if(s[0].equals(tname))
				{
					pkey=s[1];
					flag=true;
				}
								
			}
			JSONArray list = new JSONArray();    
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname+".json"));
			int len = content.size();
			if (content != null && flag==true) { 
			   for (int i=0;i<len;i++){
				   
			   obj=(JSONObject) content.get(i);
			   if(!obj.get(pkey).equals(prim))
			   {
				   list.add(obj);
			   }
			   } 
			}
			FileWriter file = new FileWriter(tname+".json");
			file.write("");	
			file.write(list.toJSONString());
			file.close();
			
		}
			
	}

	private static void inserttoTable() throws IOException, ParseException {
		
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname=sc.next();
		String name;
		String value;
		JSONParser parser = new JSONParser();
		org.json.simple.JSONObject newObj = new org.json.simple.JSONObject();
		
		if(!tablename.contains(tname))
		{
			System.out.println("Table doesn't Exist");
		}
		else{
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname+".json"));
			BufferedReader br = new BufferedReader(new FileReader(tname+"_meta.txt"));
			while((name=br.readLine())!=null){
				String s[]=name.split(" ");
				System.out.println("Enter value for "+s[0] );
				value=sc.next();
				newObj.put(s[0], value);
								
			}
			content.add(newObj);
			FileWriter file = new FileWriter(tname+".json");
			file.write("");	
			file.write(content.toJSONString());
			file.close();
		}
		
	}

	private static void listTables() {
		for(String str: tablename) {
			  System.out.println(str);
			}
		
	}

	private static void deleteTable() {
		System.out.println("Enter table name to be deleted: ");
		Scanner sc = new Scanner(System.in);
		String tname=sc.next();
		if(!tablename.contains(tname))
			System.out.println("Table does not exist");
		else{
			tablename.remove(tname);
			File file = new File(tname+"_meta.txt");
			if(file.exists())
				file.delete();
			file = new File(tname+".json");
			if(file.exists())
				file.delete();			
		}
		
	}

	private static void createTable() throws IOException {
		
		Scanner sc = new Scanner(System.in);
		int dt;
		System.out.println("Enter the Table Name");
		String tname=sc.next();
		if(tablename.contains(tname))
			System.out.println("Table already Exist");
		
		else{
			BufferedWriter br = new BufferedWriter(new FileWriter(tname+"_meta.txt"));
			System.out.println("Enter number of columns in the Table");
			int count=sc.nextInt();
			sc.reset();
			System.out.println("Available Datatypes Integer(1) Float(2) String(3) Date(4) Boolean(5)");
			for(int i=0;i<count;i++)
			{
				System.out.println("Enter Column Name");
				String line=sc.next();
				do{
				System.out.println("Enter Datatype Value e.g 1 for Integer : ");
				dt=sc.nextInt();
				}while(dt>5);
				br.write(line+" "+dt);
				br.newLine();
				
			}
			br.close();
			
			File file = new File(tname+".json");
			if(!file.exists()) {
			    file.createNewFile();
			} 
			FileWriter fw = new FileWriter(tname+".json");
			fw.write("[]");
			fw.close();
			System.out.println("What is the primary key for this Table: ");
			String primkey=sc.next();
			tablekey.put(tname, primkey);
			tablename.add(tname);
			System.out.println("New Table "+tname+" Created. ");
			
			
		}
		
	}
}
