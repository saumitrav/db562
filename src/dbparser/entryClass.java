package dbparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.lang.Object;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.IntegerComparator;
import jdbm.helper.StringComparator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

public class entryClass {

	static ArrayList<String> tablename;
	static HashMap<String, String> tablekey = new HashMap<String, String>();
	static RecordManager recman;
	static Properties props;
	static String DATABASE = "dbproj2";
	static boolean searchUsingIndex = false;
	public static void main(String[] args) throws IOException, ParseException {

		/// load file with all table names into arraylist

		File file = new File("tableList.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		String name;
		BufferedReader br = new BufferedReader(new FileReader("tableList.txt"));
		tablename = new ArrayList<String>();
		while ((name = br.readLine()) != null)
			tablename.add(name);

		br.close();

		/// load file with table name and primary key

		File prim = new File("tablekeymeta.txt");
		if (!prim.exists()) {
			prim.createNewFile();
		}

		br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		tablekey = new HashMap<String, String>();
		while ((name = br.readLine()) != null) {
			String s[] = name.split(" ");
			tablekey.put(s[0], s[1]);
		}

		br.close();
		
		props = new Properties();
		recman = RecordManagerFactory.createRecordManager(DATABASE, props );
		menu();

//		searchOnPrimKey("test1", "<", 2);
//		joinOnPkey("test1","test2");
		

		/// load all changes to file again
		FileWriter writer = new FileWriter("tableList.txt");
		writer.write("");
		writer.close();
		BufferedWriter brw = new BufferedWriter(new FileWriter("tablelist.txt"));
		for (String str : tablename) {
			brw.write(str);
			brw.newLine();
		}
		brw.close();

		/// load all changes to table primary key file
		writer = new FileWriter("tablekeymeta.txt");
		writer.write("");
		writer.close();
		BufferedWriter brw2 = new BufferedWriter(new FileWriter("tablekeymeta.txt"));

		for (Map.Entry<String, String> hm : tablekey.entrySet()) {
			String tn = hm.getKey();
			String kn = hm.getValue();
			brw2.write(tn + " " + kn);
			brw2.newLine();
			// System.out.println(hm.getKey()+ " "+ hm.getValue());
		}
		brw2.close();

	}
	

	public HashMap<String,Integer> returnDataType(String tname) throws IOException{
		HashMap<String,Integer> dtype = new HashMap<String,Integer>();
		String name;
		BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
		br.readLine();
		while ((name = br.readLine()) != null) {
			String s[] = name.split(" ");
			dtype.put(s[0], Integer.parseInt(s[1]));
		}
		br.close();
		return dtype;
	}
	
	private static void indexOnCol() throws IOException, ParseException
	{
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		String colname;
		sc.nextLine();
		String name;
		boolean flag=false;
		JSONParser parser = new JSONParser();
		long recid;
		BTree tree=new BTree();
		int type = 0;
		
		
		if (!tablename.contains(tname)) {
			 System.out.println("Table doesn't Exist");
		} else {
			
				System.out.println("Enter column name");
				colname=sc.next();
				sc.nextLine();
				BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
				br.readLine();
				while ((name = br.readLine()) != null) {
					String s[] = name.split(" ");
					if(s[0].equalsIgnoreCase(colname)){
						flag=true;
						type=Integer.parseInt(s[1]);
					}
				}
				br.close();
				//checking if column exist
				if(flag==false){
					System.out.println("No such column Exist");
					return;
				}
				//checking if its a primary key
				if(colname.equals(tname+"_pkey"))
				{
					System.out.println("Index on Primary key already exist");
					return;
				}
				//checking if tree already exist
				recid = recman.getNamedObject( tname+"_"+colname+"_btree" );
	            if ( recid != 0 ) {
	                System.out.println("Tree on "+colname+" already exist");
	                return;
	            } else {
	                if(type==1)
	                tree = BTree.createInstance( recman, new IntegerComparator() );
	                else
		                tree = BTree.createInstance( recman, new StringComparator() );
	                recman.setNamedObject( tname+"_"+colname+"_btree", tree.getRecid() );
	            }
	            //load json array
				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				if (content != null ) {
					//parsing through jsonarray
					JSONObject obj=new JSONObject();
					String cname;
					Object object;
					String forapp;
					for(Integer i=0;i<len;i++)
					{
						obj=(JSONObject) content.get(i);
						cname=obj.get(colname).toString();
						if(type==1)
						object=tree.find(Integer.parseInt(cname));
						else
							object=tree.find(cname);
						if(object==null)
						{
							if(type==1)
							tree.insert(Integer.parseInt(cname), i, false);
							else
								tree.insert(cname, i, false);
						}
						else{
							forapp=(String)object.toString();
							forapp=forapp+","+i.toString();
							if(type==1)
								tree.insert(Integer.parseInt(cname), forapp, true);
							else
							tree.insert(cname, forapp, true);
							
						}
					}
					recman.commit();
					System.out.println("Index on column "+colname+" created");
					
				}

				
				
		}
	}

	private static void recreateIndexOnCol(String tname) throws IOException, ParseException
	{
		String colname;
		
		String name;
		boolean flag=false;
		JSONParser parser = new JSONParser();
		long recid;
		BTree tree;
		int type;
		
		
			
				
				BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
				br.readLine();
				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				while ((name = br.readLine()) != null) {
					String s[] = name.split(" ");
					colname=s[0];
					type=Integer.parseInt(s[1]);
				
				
				
				//checking if tree already exist
				recid = recman.getNamedObject( tname+"_"+colname+"_btree" );
	            if ( recid != 0 ) {
	            	recman.delete(recid);
	                
	               if(type==1)
	            	   tree = BTree.createInstance( recman, new IntegerComparator() );
	               else
	                tree = BTree.createInstance( recman, new StringComparator() );
	                recman.setNamedObject( tname+"_"+colname+"_btree", tree.getRecid() );
	            
	            //load json array
				
				if (content != null ) {
					//parsing through jsonarray
					JSONObject obj=new JSONObject();
					String cname;
					Object object;
					String forapp;
					for(Integer i=0;i<len;i++)
					{
						obj=(JSONObject) content.get(i);
						cname=obj.get(colname).toString();
						if(type==1)
						object=tree.find(Integer.parseInt(cname));	
						else
						object=tree.find(cname);
						if(object==null)
						{
							if(type==1)
								tree.insert(Integer.parseInt(cname), i, false);	
							else	
							tree.insert(cname, i, false);
						}
						else{
							forapp=(String)object.toString();
							forapp=forapp+","+i.toString();
							if(type==1)
							tree.insert(Integer.parseInt(cname), forapp, true);	
							else
							tree.insert(cname, forapp, true);
							
						}
					}
					recman.commit();
					System.out.println("Index on column "+colname+" updated");
					
				}
	            }
				}

				
				
		
	}
	
	private static void createJoinTable(String tname1, String tname2) throws IOException {
		String name;
		String tablenew = tname1 + "_" + tname2 + "_temp";
		File file = new File(tname1 + "_" + tname2 + "_temp.json");
		if (file.exists())
			file.delete();
		file.createNewFile();
		file = new File(tname1 + "_" + tname2 + "_temp_meta.txt");
		if (file.exists())
			file.delete();
		file.createNewFile();
		BufferedReader br = new BufferedReader(new FileReader(tname1 + "_meta.txt"));
		BufferedWriter brw = new BufferedWriter(new FileWriter(tname1 + "_" + tname2 + "_temp_meta.txt"));
		while ((name = br.readLine()) != null) {
			brw.write(name);
			brw.newLine();
		}
		br = new BufferedReader(new FileReader(tname2 + "_meta.txt"));
		while ((name = br.readLine()) != null) {
			brw.write(name);
			brw.newLine();
		}

		brw.close();
		if (!tablename.contains(tablenew))
			tablename.add(tablenew);
		tablekey.put(tablenew, tname1 + "_pkey");
		writetoTableList();
		writetoKeyMeta();
	}
	
	public void joinOnPkey(String tname1, String tname2)
			throws FileNotFoundException, IOException, ParseException {

		JSONParser parser = new JSONParser();
		long recid1;
		long recid2;
		BTree tree1 = null;
		Tuple tuple1 = new Tuple();
		TupleBrowser browser1;
		BTree tree2 = null;
		Tuple tuple2 = new Tuple();
		TupleBrowser browser2;
		JSONArray list = new JSONArray();
		int location = Integer.MAX_VALUE;
		Object obj;

		if (!(tablename.contains(tname1) && tablename.contains(tname2))) {
			 System.out.println("Table doesn't Exist");
		} else {
			JSONArray content1 = (JSONArray) parser.parse(new FileReader(tname1 + ".json"));
			JSONArray content2 = (JSONArray) parser.parse(new FileReader(tname2 + ".json"));

			recid1 = recman.getNamedObject(tname1 + "_btree");
			if (recid1 != 0) {
				tree1 = BTree.load(recman, recid1);
			}

			recid2 = recman.getNamedObject(tname2 + "_btree");
			if (recid2 != 0) {
				tree2 = BTree.load(recman, recid2);
			}

			if (tree1 != null && tree2 != null) {
				browser1 = tree1.browse();
				createJoinTable(tname1, tname2);
				while (browser1.getNext(tuple1)) {
					location = (int) tuple1.getKey();
					obj = tree2.find(location);
					if (obj != null) {
						JSONObject object2 = (JSONObject) content2.get((int) obj);
						JSONObject object1 = (JSONObject) content1.get((int) tuple1.getValue());
						JSONObject combined = new JSONObject();
						combined.putAll(object1);
						combined.putAll(object2);
						list.add(combined);
					}
				}
				FileWriter file = new FileWriter(tname1 + "_" + tname2 + "_temp.json");
				file.write("");
				file.write(list.toJSONString());
				file.close();
			}
		}
	}
	
	public static void searchOnPrimKey(String tname, String opr, int value) throws IOException, ParseException {
		long recid;
		BTree tree = null;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Object obj = null;
		JSONParser parser = new JSONParser();
		int location = Integer.MAX_VALUE;
		if (!tablename.contains(tname)) {
			// System.out.println("Table doesn't Exist");
		} else {
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray list = new JSONArray();
			JSONObject object;
			if (opr.equals("<") || opr.equals(">") || opr.equals("=")) {
				if (opr.equals("=")) {
					recid = recman.getNamedObject(tname + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						obj = tree.find(value);
						if (obj != null)
							location = (int) obj;
						else {
							// createTempTable(tname, tname+"_temp");
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
						if (location < content.size()) {
							object = (JSONObject) content.get(location);
							// createTempTable(tname, tname+"_temp");
							list.add(object);
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
					}
				} else if (opr.equals(">")) {
					recid = recman.getNamedObject(tname + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						// createTempTable(tname, tname+"_temp");
						while (browser.getNext(tuple)) {
							location = (int) tuple.getValue();
							int tempkey = (int) tuple.getKey();
							if (location < content.size() && tempkey > value) {
								object = (JSONObject) content.get(location);
								list.add(object);
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					}
				} else if (opr.equals("<")) {
					recid = recman.getNamedObject(tname + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						// createTempTable(tname, tname+"_temp");
						while (browser.getPrevious(tuple)) {
							location = (int) tuple.getValue();
							int tempkey = (int) tuple.getKey();
							if (location < content.size() && tempkey < value) {
								object = (JSONObject) content.get(location);
								list.add(object);
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					}else{
						System.out.println("Btree doesn't exist.");
						return;
					}
				}
			} else {
				// System.out.println("invalid operation");
			}
		}
	}
	
	public int searchOnIndexedKey(String tname, String attribute, String opr, int value)
			throws IOException, ParseException {
		long recid;
		BTree tree = null;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Object obj = null;
		JSONParser parser = new JSONParser();
		int location = Integer.MAX_VALUE;
		if (!tablename.contains(tname)) {
			// System.out.println("Table doesn't Exist");
		} else {
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray content2 = (JSONArray) parser.parse(new FileReader(tname + "_temp.json"));
			JSONArray list = new JSONArray();
			JSONObject object;
			if (opr.equals("<") || opr.equals(">") || opr.equals("=")) {
				if (opr.equals("=")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						obj = tree.find(tempp);
						if (obj != null) {
							if (obj.toString().contains(",")) {
								String[] objLocs = obj.toString().split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										if (location < content.size()) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
										}
									}
								}
								FileWriter file = new FileWriter(tname + "_temp.json");
								file.write("");
								file.write(list.toJSONString());
								file.close();
								
							} else {
								location = Integer.parseInt(obj.toString().trim());
								if (location < content.size()) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
									FileWriter file = new FileWriter(tname + "_temp.json");
									file.write("");
									file.write(list.toJSONString());
									file.close();
								}
							}
						} else {
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals(">")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						browser = tree.browse(tempp);
						while (browser.getNext(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										int tempkey = Integer.parseInt(tuple.getKey().toString());
										if (location < content.size() && tempkey > value) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								int tempkey = Integer.parseInt(tuple.getKey().toString());
								if (location < content.size() && tempkey > value) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals("<")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						browser = tree.browse(tempp);
						while (browser.getPrevious(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										int tempkey = Integer.parseInt(tuple.getKey().toString());
										if (location < content.size() && tempkey < value) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								int tempkey = Integer.parseInt(tuple.getKey().toString());
								if (location < content.size() && tempkey < value) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else {
					 System.out.println("invalid operation");
				}
			}
		}
		return 0;
	}
	
	public int searchOnIndexedKey2(String tname, String attribute, String opr, String value)
			throws IOException, ParseException {
		long recid;
		BTree tree = null;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Object obj = null;
		JSONParser parser = new JSONParser();
		int location = Integer.MAX_VALUE;
		if (!tablename.contains(tname)) {
			// System.out.println("Table doesn't Exist");
		} else {
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray content2 = (JSONArray) parser.parse(new FileReader(tname + "_temp.json"));
			JSONArray list = new JSONArray();
			JSONObject object;
			if (opr.equals("<") || opr.equals(">") || opr.equals("=")) {
				if (opr.equals("=")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						obj = tree.find(value);
						if (obj != null) {
							if (obj.toString().contains(",")) {
								String[] objLocs = obj.toString().split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										if (location < content.size()) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
											
										}
									}
								}
								FileWriter file = new FileWriter(tname + "_temp.json");
								file.write("");
								file.write(list.toJSONString());
								file.close();
								
							} else {
								location = Integer.parseInt(obj.toString().trim());
								if (location < content.size()) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
									FileWriter file = new FileWriter(tname + "_temp.json");
									file.write("");
									file.write(list.toJSONString());
									file.close();
								}
							}
						} else {
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals(">")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						while (browser.getNext(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										String tempkey = tuple.getKey().toString();
										if (location < content.size() && tempkey.compareTo(value)>0) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								String tempkey = (tuple.getKey().toString());
								if (location < content.size() && tempkey.compareTo(value)>0) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals("<")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						while (browser.getPrevious(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										String tempkey = (tuple.getKey().toString());
										if (location < content.size() && tempkey.compareTo(value)<0) {
											object = (JSONObject) content.get(location);
											if(content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								String tempkey = (tuple.getKey().toString());
								if (location < content.size() && tempkey.compareTo(value)<0) {
									object = (JSONObject) content.get(location);
									if(content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else {
					 System.out.println("invalid operation");
				}
			}
		}
		return 0;
	}

	public int searchOnIndexedKey3(String tname, String attribute, String opr, int value)
			throws IOException, ParseException {
		long recid;
		BTree tree = null;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Object obj = null;
		JSONParser parser = new JSONParser();
		int location = Integer.MAX_VALUE;
		if (!tablename.contains(tname)) {
			// System.out.println("Table doesn't Exist");
		} else {
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray content2 = (JSONArray) parser.parse(new FileReader(tname + "_temp.json"));
			if(content.equals(content2)){
				content2=new JSONArray();
			}
			JSONArray list = new JSONArray();
			for(Object o:content2){
				list.add((JSONObject)o);
			}
			JSONObject object;
			if (opr.equals("<") || opr.equals(">") || opr.equals("=")) {
				if (opr.equals("=")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						obj = tree.find(tempp);
						if (obj != null) {
							if (obj.toString().contains(",")) {
								String[] objLocs = obj.toString().split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										if (location < content.size()) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
										}
									}
								}
								FileWriter file = new FileWriter(tname + "_temp.json");
								file.write("");
								file.write(list.toJSONString());
								file.close();
								
							} else {
								location = Integer.parseInt(obj.toString().trim());
								if (location < content.size()) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
									FileWriter file = new FileWriter(tname + "_temp.json");
									file.write("");
									file.write(list.toJSONString());
									file.close();
								}
							}
						} else {
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals(">")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						browser = tree.browse(tempp);
						while (browser.getNext(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										int tempkey = Integer.parseInt(tuple.getKey().toString());
										if (location < content.size() && tempkey > value) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								int tempkey = Integer.parseInt(tuple.getKey().toString());
								if (location < content.size() && tempkey > value) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals("<")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						Integer tempp = value;
						browser = tree.browse(tempp);
						while (browser.getPrevious(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										int tempkey = Integer.parseInt(tuple.getKey().toString());
										if (location < content.size() && tempkey < value) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								int tempkey = Integer.parseInt(tuple.getKey().toString());
								if (location < content.size() && tempkey < value) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else {
					 System.out.println("invalid operation");
				}
			}
		}
		return 0;
	}
	
	public int searchOnIndexedKey4(String tname, String attribute, String opr, String value)
			throws IOException, ParseException {
		long recid;
		BTree tree = null;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Object obj = null;
		JSONParser parser = new JSONParser();
		int location = Integer.MAX_VALUE;
		if (!tablename.contains(tname)) {
			// System.out.println("Table doesn't Exist");
		} else {
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray content2 = (JSONArray) parser.parse(new FileReader(tname + "_temp.json"));
			if(content.equals(content2)){
				content2=new JSONArray();
			}
			JSONArray list = new JSONArray();
			for(Object o:content2){
				list.add((JSONObject)o);
			}
			JSONObject object;
			if (opr.equals("<") || opr.equals(">") || opr.equals("=")) {
				if (opr.equals("=")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						obj = tree.find(value);
						if (obj != null) {
							if (obj.toString().contains(",")) {
								String[] objLocs = obj.toString().split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										if (location < content.size()) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
											
										}
									}
								}
								FileWriter file = new FileWriter(tname + "_temp.json");
								file.write("");
								file.write(list.toJSONString());
								file.close();
								
							} else {
								location = Integer.parseInt(obj.toString().trim());
								if (location < content.size()) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
									FileWriter file = new FileWriter(tname + "_temp.json");
									file.write("");
									file.write(list.toJSONString());
									file.close();
								}
							}
						} else {
							FileWriter file = new FileWriter(tname + "_temp.json");
							file.write("");
							file.write(list.toJSONString());
							file.close();
						}
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals(">")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						while (browser.getNext(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										String tempkey = tuple.getKey().toString();
										if (location < content.size() && tempkey.compareTo(value)>0) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								String tempkey = (tuple.getKey().toString());
								if (location < content.size() && tempkey.compareTo(value)>0) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else if (opr.equals("<")) {
					recid = recman.getNamedObject(tname + "_" + attribute + "_btree");
					if (recid != 0) {
						tree = BTree.load(recman, recid);
					}
					if (tree != null) {
						browser = tree.browse(value);
						while (browser.getPrevious(tuple)) {
							String temp = tuple.getValue().toString();
							if (temp.contains(",")) {
								String[] objLocs = temp.split(",");
								for (String str : objLocs) {
									if (!str.trim().equals("")) {
										location = Integer.parseInt(str.trim());
										String tempkey = (tuple.getKey().toString());
										if (location < content.size() && tempkey.compareTo(value)<0) {
											object = (JSONObject) content.get(location);
											if(!content2.contains(object))
											list.add(object);
										}
									}
								}
							} else {
								location = Integer.parseInt(temp.trim());
								String tempkey = (tuple.getKey().toString());
								if (location < content.size() && tempkey.compareTo(value)<0) {
									object = (JSONObject) content.get(location);
									if(!content2.contains(object))
									list.add(object);
								}
							}
						}
						FileWriter file = new FileWriter(tname + "_temp.json");
						file.write("");
						file.write(list.toJSONString());
						file.close();
					} else {
						System.out.println("Tree not present");
						return -1;
					}
				} else {
					 System.out.println("invalid operation");
				}
			}
		}
		return 0;
	}

	private static void writetoKeyMeta() throws IOException {
		FileWriter writer = new FileWriter("tablekeymeta.txt");
		writer.write("");
		writer.close();
		BufferedWriter brw2 = new BufferedWriter(new FileWriter("tablekeymeta.txt"));

		for (Map.Entry<String, String> hm : tablekey.entrySet()) {
			String tn = hm.getKey();
			String kn = hm.getValue();
			brw2.write(tn + " " + kn);
			brw2.newLine();
			// System.out.println(hm.getKey()+ " "+ hm.getValue());
		}
		brw2.close();		
	}

	private static void writetoTableList() throws IOException {
		
		FileWriter writer = new FileWriter("tableList.txt");
		writer.write("");
		writer.close();
		BufferedWriter brw = new BufferedWriter(new FileWriter("tablelist.txt"));
		for (String str : tablename) {
			brw.write(str);
			brw.newLine();
		}
		brw.close();
		
	}

	private static void printColNames(ArrayList<String> list) {
		System.out.println();
		for (int i = 0; i < list.size(); i++) {
//			System.out.print(list.get(i) + "\t\t\t");
			System.out.printf("%-23s",list.get(i));
			System.out.print("|");
		}
		System.out.println();
		for (int i = 0; i < list.size(); i++) {
//			System.out.print(list.get(i) + "\t\t\t");
			System.out.print("------------------------");
		}
		System.out.println();
	}

	private static void printObj(ArrayList<String> list, JSONObject obj) {
		for (int j = 0; j < list.size(); j++) {
			System.out.print(obj.get(list.get(j)) + "\t\t\t\t");
		}
		System.out.println();
	}
	
	void printTable(String tname) throws FileNotFoundException, IOException, ParseException {
		String name;
		ArrayList<String> list = new ArrayList<>();
		JSONObject obj = new JSONObject();
		JSONParser parser = new JSONParser();
		
		BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
		while ((name = br.readLine()) != null) {
			String s[] = name.split(" ");
			list.add(s[0]);
		}
		br.close();
		
		JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
		int len = content.size();
		
		if (content != null) {
			printColNames(list);
			for (int i = 0; i < len; i++) {
				obj = (JSONObject) content.get(i);
				for (int j = 0; j < list.size(); j++) {
						System.out.print(obj.get(list.get(j)) + "\t\t\t");
				}
				System.out.println();
			}
		}
	}

	private static void menu() throws IOException, ParseException {

		String option;
		do {
			System.out.println("Movie Database System ");
			System.out.println("MENU ");
			System.out.println("1)Create New Table\n2)Delete Existing Table\n3)List all tables in Database");
			System.out.println(
					"4)Insert Record into a Table\n5)Delete Record from a Table\n6)Update Record from a Table\n7)List content of a Table");
			System.out.println(
					"8)Search Record for an Attribute\n9)Sort Table\n10)Print B+ tree\n11)Enter SQL command\n12)Mass insert");
			System.out.println("13)Create Index on column\n14)Enter SQL command for search on indexed attributes\n15)Exit");
			System.out.print("\nEnter your choice : ");
			Scanner scan = new Scanner(System.in);
			option = scan.next();
			parser parse = new parser();
			

			switch (option) {
			case "1":
				createTable();
				break;
			case "2":
				deleteTable();
				break;
			case "3":
				listTables();
				break;
			case "4":
				inserttoTable();
				break;
			case "5":
				deletefromTable();
				break;
			case "6":
				updatefromTable();
				break;
			case "7":
				listfromTable();
				break;
			case "8":
				searchFromTable();
				break;
			case "9":
				sortTable();
				break;
			case "10":
				printBtree();
				break;
			case "11":
				String inSQL;
				System.out.print("\nEnter the SQL statement : ");
				scan.nextLine();
				inSQL = scan.nextLine();
				long startTime = System.nanoTime();
//				parser parse = new parser();
				parse.parseSQL(inSQL);
				long endTime = System.nanoTime();
				System.out.println("\nTook " + (endTime - startTime) / 1000000 + " milliseconds.");
				break;
			case "12":
				massInsert();
				break;
			case "13":
				indexOnCol();
				break;
			case "14":
				String inSQL2;
				System.out.print("\nEnter the SQL statement : ");
				scan.nextLine();
				inSQL2 = scan.nextLine();
				long startTime2 = System.nanoTime();
//				parser parse = new parser();
				searchUsingIndex = true;
				parse.parseSQL(inSQL2);
				long endTime2 = System.nanoTime();
				System.out.println("\nTook " + (endTime2 - startTime2) / 1000000 + " milliseconds.");
				searchUsingIndex = false;
				break;
			case "15":
				break;
			default:
				System.out.println("Invalid Entry");
				break;

			}
			if (!option.equals("15")) {
				System.out.println("\nPress ENTER to Continue");
				System.in.read();
			}

		} while (!option.equals("15"));
	}

	private static void massInsert() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		String pkey=tname+"_pkey";
		JSONParser parser = new JSONParser();
		String name;
		Random rn = new Random();
		Object value;
		String longstring="abcdefghijklmnopqrstuvwxyz";
		long recid;
		BTree tree=null;
		

		
		if (!tablename.contains(tname)) 
			System.out.println("Table doesn't Exist");
		
		else{
			
				//read content for unique key value
				BufferedReader filekey = new BufferedReader(new FileReader(tname+"_unique.txt"));
				String keyline = filekey.readLine();
					String sunique[] = keyline.split(" ");
					Integer uniquekey=Integer.parseInt(sunique[0]); 								
				filekey.close();
				
				//
				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				recid = recman.getNamedObject( tname+"_btree" );
	            if ( recid != 0 ) {
	                tree = BTree.load( recman, recid );}
				do{
				
				int len = content.size();				
				BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
				br.readLine();
				JSONObject newObj=new JSONObject();
				newObj.put(tname+"_pkey", uniquekey);
				uniquekey++;
				while ((name = br.readLine()) != null) {
					String s[] = name.split(" ");
					//System.out.println("Enter value for " + s[0]);
					// type check start
					if (Integer.parseInt(s[1]) == 1) {
						value=rn.nextInt(1000 - 1) + 1;
					}
					else if (Integer.parseInt(s[1]) == 2) {
						value=rn.nextFloat() * (10.0f - 1.0f) + 1.0f;
					}
					else if (Integer.parseInt(s[1]) == 5) {
						value=rn.nextBoolean();
					} 
					else if (Integer.parseInt(s[1]) == 4) {
						DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
						Integer date =rn.nextInt(28-1)+1;
						Integer month=rn.nextInt(12-1)+1;
						Integer year = rn.nextInt(2999-1867)+1867;
						value= date.toString()+"-"+month.toString()+"-"+year.toString();
					} 
					else {
						String temp="";
						for(int i=0;i<8;i++)
							temp=temp+longstring.charAt(rn.nextInt(25-1)+1);
						value =temp;
					}
					// type check ends
					newObj.put(s[0], value);
				}
				br.close();
				
					content.add(newObj);
					
				
				//
				// writing to b tree
					if(tree!=null)
	                tree.insert( uniquekey-1, content.size()-1, false );
					
	                
	           
				//
				}while(uniquekey<100000);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				System.out.println("\nRecord added successfully.");
			//}
			//write file content to unique file
			FileWriter writerunique = new FileWriter(tname+"_unique.txt");
			writerunique.write("");
			writerunique.write(uniquekey.toString());
			writerunique.close();
			recman.commit();
		}
		
		
	}

	private static void printBtreeArg(String tname) throws IOException {

		long recid;
		BTree tree;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		Scanner sc = new Scanner(System.in);
		String colname;
		if (!tablename.contains(tname))
			System.out.println("Table doesn't Exist");
		else {
			System.out.println("Enter column name or P for primary key");
			colname = sc.next();
			sc.nextLine();
			if (colname.equalsIgnoreCase("p")) {
				recid = recman.getNamedObject(tname + "_btree");
				if (recid != 0) {
					tree = BTree.load(recman, recid);
					System.out.println("Loaded BTree " + tname + " size " + tree.size());

					browser = tree.browse();
					while (browser.getNext(tuple)) {
						System.out.println(tuple.getKey() + " " + tuple.getValue());
					}
				}
			} else {
				recid = recman.getNamedObject(tname + "_" + colname + "_btree");
				if (recid != 0) {
					tree = BTree.load(recman, recid);
					System.out.println("Loaded BTree " + tname + " size " + tree.size());

					browser = tree.browse();
					while (browser.getNext(tuple)) {
						System.out.println(tuple.getKey() + " " + tuple.getValue());
					}
				} else {
					System.out.println("Index on " + colname + " doesnt exist");
				}
			}
		}
	}

	private static void printBtree() throws IOException {
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter name of the table");
		String tname=scan.next();
		scan.nextLine();
		printBtreeArg(tname);
		
	}

	private static void sortTable() throws IOException, ParseException {
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			String name;
			String pkey = "";
			boolean flag = false;
			boolean flag2 = false;

			// find primary key of the table
			BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
					flag = true;
				}
			}
			br.close();

			if (flag) {
				ArrayList<String> list = new ArrayList<>();
				ArrayList<String> sortedListStr = new ArrayList<>();
				ArrayList<Integer> sortedListInt = new ArrayList<>();
				ArrayList<Float> sortedListFlo = new ArrayList<>();
				JSONParser parser = new JSONParser();
				JSONObject obj = new JSONObject();
				BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));
				int checkDataType = 0;

				// get the fields in the table into list
				while ((name = br2.readLine()) != null) {
					String s[] = name.split(" ");
					list.add(s[0]);
					if (s[0].equals(pkey)) {
	                    checkDataType = Integer.parseInt(s[1]);
	                }
				}
				br2.close();

				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				if (content != null) {
					for (int i = 0; i < len; i++) {
						obj = (JSONObject) content.get(i);
						if(checkDataType == 1){
							sortedListInt.add(Integer.parseInt(obj.get(pkey).toString()));
						}else if(checkDataType == 2){
							sortedListFlo.add(Float.parseFloat(obj.get(pkey).toString()));
						}else{
							sortedListStr.add(obj.get(pkey).toString());
						}
					}
					System.out.println("Enter the following for displaying the contents sorted based on primary key:");
					while (true) {
						System.out.println("1 for ascending order, 2 for descending order: ");
						String choice = sc.next();

						if (choice.equals("1")) {
							if(checkDataType == 1){
								Collections.sort(sortedListInt);
							}else if(checkDataType == 2){
								Collections.sort(sortedListFlo);
							}else{
								Collections.sort(sortedListStr);
							}
							break;
						} else if (choice.equals("2")) {
							if(checkDataType == 1){
								Collections.sort(sortedListInt, Collections.reverseOrder());
							}else if(checkDataType == 2){
								Collections.sort(sortedListFlo, Collections.reverseOrder());
							}else{
								Collections.sort(sortedListStr, Collections.reverseOrder());
							}
							break;
						} else {
							System.out.println("Enter either 1 or 2!");
						}
					}

//					for (int i = 0; i < list.size(); i++) {
//						System.out.print(list.get(i) + "\t\t\t\t\t");
//					}
//					System.out.println();
					printColNames(list);
					

					for (int i = 0; i < len; i++) {
						for (int k = 0; k < len; k++) {
							obj = (JSONObject) content.get(k);

							if (checkDataType == 1) {
								if (obj.get(pkey).toString().equals(sortedListInt.get(i).toString())) {
									flag2 = true;
									break;
								}
							} else if (checkDataType == 2) {
								if (obj.get(pkey).toString().equals(sortedListFlo.get(i).toString())) {
									flag2 = true;
									break;
								}
							} else {
								if (obj.get(pkey).toString().equals(sortedListStr.get(i).toString())) {
									flag2 = true;
									break;
								}
							}
						}
						if (flag2) {
//							for (int j = 0; j < list.size(); j++) {
//								System.out.print(obj.get(list.get(j)) + "\t\t\t");
//							}
//							System.out.println();
							printObj(list, obj);
						} else {
							System.out.print("\t\t\tSomething wrong with the database!!");
						}
					}
				}
			}
		}
	}
	
	public ArrayList<String> getPkeys(String tname) throws IOException, ParseException {
		ArrayList<String> pkeys = new ArrayList<String>();
		JSONParser parser = new JSONParser();
		JSONObject obj = new JSONObject();
		String name = "";
		String pkey = "";

		// find primary key of the table
		BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		while ((name = br.readLine()) != null) {
			String s[] = name.split(" ");
			if (s[0].equals(tname)) {
				pkey = s[1];
			}
		}
		br.close();

		JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
		int len = content.size();
		if (content != null) {
			for (int i = 0; i < len; i++) {
				obj = (JSONObject) content.get(i);
				if (!obj.get(pkey).toString().equals("")) {
					pkeys.add(obj.get(pkey).toString());
				}
			}
		}
		return pkeys;
	}

	void sortTableSQL(String tname, String orderBy, boolean ascending) throws IOException, ParseException {
		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			String name;
			JSONParser parser = new JSONParser();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			JSONArray content2 = new JSONArray();

			ArrayList<String> list = new ArrayList<>();
			ArrayList<String> sortedListStr = new ArrayList<>();
			ArrayList<Integer> sortedListInt = new ArrayList<>();
			ArrayList<Float> sortedListFlo = new ArrayList<>();
			JSONObject obj = new JSONObject();
			BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));
			int checkDataType = 0;

			// check data type of order by attribute
			while ((name = br2.readLine()) != null) {
				String s[] = name.split(" ");
				list.add(s[0]);
				if (s[0].equals(orderBy)) {
					checkDataType = Integer.parseInt(s[1]);
				}
			}
			br2.close();

			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					if (checkDataType == 1) {
						sortedListInt.add(Integer.parseInt(obj.get(orderBy).toString()));
					} else if (checkDataType == 2) {
						sortedListFlo.add(Float.parseFloat(obj.get(orderBy).toString()));
					} else {
						sortedListStr.add(obj.get(orderBy).toString());
					}
				}

				if (ascending) {
					if (checkDataType == 1) {
						Collections.sort(sortedListInt);
					} else if (checkDataType == 2) {
						Collections.sort(sortedListFlo);
					} else {
						Collections.sort(sortedListStr);
					}
				} else {
					if (checkDataType == 1) {
						Collections.sort(sortedListInt, Collections.reverseOrder());
					} else if (checkDataType == 2) {
						Collections.sort(sortedListFlo, Collections.reverseOrder());
					} else {
						Collections.sort(sortedListStr, Collections.reverseOrder());
					}
				}

//				printColNames(list);

				for (int i = 0; i < len; i++) {
					for (int k = 0; k < content.size(); k++) {
						obj = (JSONObject) content.get(k);

						if (checkDataType == 1) {
							if (obj.get(orderBy).toString().equals(sortedListInt.get(i).toString())) {
								break;
							}
						} else if (checkDataType == 2) {
							if (obj.get(orderBy).toString().equals(sortedListFlo.get(i).toString())) {
								break;
							}
						} else {
							if (obj.get(orderBy).toString().equals(sortedListStr.get(i).toString())) {
								break;
							}
						}
					}
					content.remove(obj);
//					printObj(list, obj);
					content2.add(obj);
				}
				FileWriter file = new FileWriter(tname + ".json");
				file.write(content2.toJSONString());
				file.close();
			}
		}
	}

	public String getPkey(String table){
		if(tablekey.containsKey(table)){
			return tablekey.get(table);
		}
		return "-1";
	}
	
	private static void sortTablelist(String tname, JSONArray content, ArrayList<String> colname) throws IOException, ParseException {
//		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
//		String tname = sc.next();
		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			String name;
			String pkey = "";
			boolean flag = false;
			boolean flag2 = false;

			// find primary key of the table
			BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
					flag = true;
				}
			}
			br.close();

			if (flag) {
				ArrayList<String> list = new ArrayList<>();
				ArrayList<String> sortedListStr = new ArrayList<>();
				ArrayList<Integer> sortedListInt = new ArrayList<>();
				ArrayList<Float> sortedListFlo = new ArrayList<>();
				JSONParser parser = new JSONParser();
				JSONObject obj = new JSONObject();
				BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));
				int checkDataType = 0;

				// get the fields in the table into list
				while ((name = br2.readLine()) != null) {
					String s[] = name.split(" ");
					//list.add(s[0]);
					if (s[0].equals(pkey)) {
	                    checkDataType = Integer.parseInt(s[1]);
	                }
				}
				br2.close();

//				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				if (content != null) {
					for (int i = 0; i < len; i++) {
						obj = (JSONObject) content.get(i);
						if(checkDataType == 1){
							sortedListInt.add(Integer.parseInt(obj.get(pkey).toString()));
						}else if(checkDataType == 2){
							sortedListFlo.add(Float.parseFloat(obj.get(pkey).toString()));
						}else{
							sortedListStr.add(obj.get(pkey).toString());
						}
					}
					System.out.println("Enter the following for displaying the contents sorted based on primary key:");
					while (true) {
						System.out.println("1 for ascending order, 2 for descending order: ");
						String choice = sc.next();

						if (choice.equals("1")) {
							if(checkDataType == 1){
								Collections.sort(sortedListInt);
							}else if(checkDataType == 2){
								Collections.sort(sortedListFlo);
							}else{
								Collections.sort(sortedListStr);
							}
							break;
						} else if (choice.equals("2")) {
							if(checkDataType == 1){
								Collections.sort(sortedListInt, Collections.reverseOrder());
							}else if(checkDataType == 2){
								Collections.sort(sortedListFlo, Collections.reverseOrder());
							}else{
								Collections.sort(sortedListStr, Collections.reverseOrder());
							}
							break;
						} else {
							System.out.println("Enter either 1 or 2!");
						}
					}

//					for (int i = 0; i < list.size(); i++) {
//						System.out.print(list.get(i) + "\t\t\t\t\t");
//					}
//					System.out.println();
					printColNames(colname);
					

					for (int i = 0; i < len; i++) {
						for (int k = 0; k < len; k++) {
							obj = (JSONObject) content.get(k);

							if (checkDataType == 1) {
								if (obj.get(pkey).toString().equals(sortedListInt.get(i).toString())) {
									flag2 = true;
									break;
								}
							} else if (checkDataType == 2) {
								if (obj.get(pkey).toString().equals(sortedListFlo.get(i).toString())) {
									flag2 = true;
									break;
								}
							} else {
								if (obj.get(pkey).toString().equals(sortedListStr.get(i).toString())) {
									flag2 = true;
									break;
								}
							}
						}
						if (flag2) {
//							for (int j = 0; j < list.size(); j++) {
//								System.out.print(obj.get(list.get(j)) + "\t\t\t");
//							}
//							System.out.println();
							printObj(colname, obj);
						} else {
							System.out.print("\t\t\tSomething wrong with the database!!");
						}
					}
				}
			}
		}
	}
	
	private static void deleteRow(String tname, String pkey, String pkeyVal) throws IOException, ParseException {
		JSONParser parser = new JSONParser();
		JSONObject obj;
		long recid;
		BTree tree=null;
		int prim = Integer.parseInt(pkeyVal);
		int location = Integer.MAX_VALUE;
		Tuple tuple = new Tuple();
		TupleBrowser browser;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {

			recid = recman.getNamedObject(tname+"_btree");
			if (recid != 0) {
				tree = BTree.load(recman, recid);
			}
			Object loc = tree.find(prim);
			if (loc != null) {
				location = (int) tree.find(prim);
			}
			else return;
			if (recid != 0) {
//			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && location < len) {
//				for (int i = 0; i < len; i++) {
//					obj = (JSONObject) content.get(i);
//					if (!obj.get(pkey).toString().equals(pkeyVal)) {
//						list.add(obj);
//					}
//				}
				content.remove(location);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				
				browser=tree.browse();
				while(browser.getNext(tuple)){
					if((int)tuple.getKey()>prim){
						tree.insert(tuple.getKey(), (int)tuple.getValue()-1, true);
					}
				}
				tree.remove(prim);
				recman.commit();
			}
			System.out.println("\n1 record deleted.");
			recreateIndexOnCol(tname);
			}else{
				System.out.println("\nPrimary key " + pkeyVal + "not found.");
			}
		}
	}
	
	public void deleteForOrSQL(String tableName, ArrayList<String> whereConds) throws IOException, ParseException {
		String line = "";
		String pkey = "";
		String temptname = tableName + "_temp";
		JSONParser parser = new JSONParser();
		ArrayList<String> pkeys = new ArrayList<String>();

		for (String cond : whereConds) {
			String delims = "((?<=>|<|=)|(?=>|<|=))";
			String[] currCond = cond.split(delims);
			String columnName = currCond[0].trim();
			String operator = currCond[1].trim();
			String valueToSearch = currCond[2].trim();
			if (valueToSearch.contains("'")) {
				valueToSearch = valueToSearch.replace("'", "");
			}

			JSONArray content = null;
//			JSONArray content2 = new JSONArray();
			JSONObject obj;
			boolean flag = false;
			boolean operatorFlag = false;
			int checkDataType = 0;

			if (operator.equals("=") || operator.equals("<") || operator.equals(">")) {
				operatorFlag = true;
			} else {
				System.out.println("Where operator invalid!");
				return;
			}

			if (!tablename.contains(temptname)) {
				System.out.println("Table does not exist!");
			} else {
				try {
					BufferedReader bufferedReader = new BufferedReader(new FileReader(temptname + "_meta.txt"));
					boolean columnNameFlag = false;
					while ((line = bufferedReader.readLine()) != null) {
						String s[] = line.split(" ");
						if (s[0].equals(columnName)) {
							columnNameFlag = true;
							int temp = Integer.parseInt(s[1]);
							if (temp == 1) {
								flag = true;
								checkDataType = 1;
							} else if (temp == 2) {
								flag = true;
								checkDataType = 2;
							}
						}
					}
					bufferedReader.close();

					if (columnNameFlag == false) {
						System.out.println(
								"The column name: " + columnName + " does not exist in given table: " + temptname);
					} else {
						try {
							content = (JSONArray) parser.parse(new FileReader(temptname + ".json"));
						} catch (ParseException e) {
							System.out.println("ParseException in search method");
							e.printStackTrace();
						}
						int len = content.size();
						if (content != null && operatorFlag == true) {
							if (flag == false) {

								// handling for string data types
								for (int i = 0; i < len; i++) {
									obj = (JSONObject) content.get(i);
									if (obj.get(columnName).toString().equals(valueToSearch)) {
//										content2.add(obj);
										if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
											pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
										}
									}
								}
							} else {
								// handling for Integer
								if (checkDataType == 1) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) < Integer
														.parseInt(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) > Integer
														.parseInt(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) == Integer
														.parseInt(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " +
										// valueToSearch + " does not exist in
										// this table");
									}

								} else if (checkDataType == 2) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) < Float
														.parseFloat(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) > Float
														.parseFloat(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) == Float
														.parseFloat(valueToSearch)) {
//													content2.add(obj);
													if(!pkeys.contains(obj.get(temptname.replace("_temp", "")+"_pkey").toString())){
														pkeys.add(obj.get(temptname.replace("_temp", "")+"_pkey").toString());
													}
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " +
										// valueToSearch + " does not exist in
										// this table");
									}
								}
							}
//							FileWriter file = new FileWriter(temptname + ".json");
//							file.write(content2.toJSONString());
//							file.close();
						} else {
							System.out.println(
									"Unable to perform search. Please enter table name, column name and operator values correctly");
						}
					}

				} catch (FileNotFoundException e) {
					System.out.println("Unable to read file tablekeymeta.txt in search method");
					e.printStackTrace();
				} catch (IOException io) {
					System.out.println("IOException in search method");
					io.printStackTrace();
				}
			}
		}

		// finding pkey
//		boolean pkeyFlag = false;
//		JSONObject obj2;
//		ArrayList<String> pkeyList = new ArrayList<>();
//		BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
//		while ((line = br.readLine()) != null) {
//			String s[] = line.split(" ");
//			if (s[0].equals(tableName)) {
//				pkey = s[1];
//				pkeyFlag = true;
//				break;
//			}
//		}
//		br.close();

		// calling delete row on the primary keys found
//		if (pkeyFlag) {
//			JSONArray content = (JSONArray) parser.parse(new FileReader(temptname + ".json"));
//			int len = content.size();
//			if (content != null) {
//				for (int i = 0; i < len; i++) {
//					obj2 = (JSONObject) content.get(i);
//					pkeyList.add(obj2.get(pkey).toString());
//				}
//			}
//		}
		
		for(String str:pkeys){
			deleteRow(tableName, tableName+"_pkey", str);
		}
	}
	
	public void deleteForSQL(String tableName, ArrayList<String> whereConds) throws IOException, ParseException {
		String line = "";
		String pkey = "";
		String temptname = tableName + "_temp";
		JSONParser parser = new JSONParser();

		for (String cond : whereConds) {
			String delims = "((?<=>|<|=)|(?=>|<|=))";
			String[] currCond = cond.split(delims);
			String columnName = currCond[0].trim();
			String operator = currCond[1].trim();
			String valueToSearch = currCond[2].trim();
			if (valueToSearch.contains("'")) {
				valueToSearch = valueToSearch.replace("'", "");
			}

			JSONArray content = null;
			JSONArray content2 = new JSONArray();
			JSONObject obj;
			boolean flag = false;
			boolean operatorFlag = false;
			int checkDataType = 0;

			if (operator.equals("=") || operator.equals("<") || operator.equals(">")) {
				operatorFlag = true;
			} else {
				System.out.println("Where operator invalid!");
			}

			if (!tablename.contains(temptname)) {
				System.out.println("Table does not exist!");
			} else {
				try {
					BufferedReader bufferedReader = new BufferedReader(new FileReader(temptname + "_meta.txt"));
					boolean columnNameFlag = false;
					while ((line = bufferedReader.readLine()) != null) {
						String s[] = line.split(" ");
						if (s[0].equals(columnName)) {
							columnNameFlag = true;
							int temp = Integer.parseInt(s[1]);
							if (temp == 1) {
								flag = true;
								checkDataType = 1;
							} else if (temp == 2) {
								flag = true;
								checkDataType = 2;
							}
						}
					}
					bufferedReader.close();

					if (columnNameFlag == false) {
						System.out.println(
								"The column name: " + columnName + " does not exist in given table: " + temptname);
					} else {
						try {
							content = (JSONArray) parser.parse(new FileReader(temptname + ".json"));
						} catch (ParseException e) {
							System.out.println("ParseException in search method");
							e.printStackTrace();
						}
						int len = content.size();
						if (content != null && operatorFlag == true) {
							if (flag == false) {

								// handling for string data types
								for (int i = 0; i < len; i++) {
									obj = (JSONObject) content.get(i);
									if (obj.get(columnName).toString().equals(valueToSearch)) {
										content2.add(obj);
									}
								}
							} else {
								// handling for Integer
								if (checkDataType == 1) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) < Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) > Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) == Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " +
										// valueToSearch + " does not exist in
										// this table");
									}

								} else if (checkDataType == 2) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) < Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) > Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) == Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " +
										// valueToSearch + " does not exist in
										// this table");
									}
								}
							}
							FileWriter file = new FileWriter(temptname + ".json");
							file.write(content2.toJSONString());
							file.close();
						} else {
							System.out.println(
									"Unable to perform search. Please enter table name, column name and operator values correctly");
						}
					}

				} catch (FileNotFoundException e) {
					System.out.println("Unable to read file tablekeymeta.txt in search method");
					e.printStackTrace();
				} catch (IOException io) {
					System.out.println("IOException in search method");
					io.printStackTrace();
				}
			}
		}

		// finding pkey
		boolean pkeyFlag = false;
		JSONObject obj2;
		ArrayList<String> pkeyList = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		while ((line = br.readLine()) != null) {
			String s[] = line.split(" ");
			if (s[0].equals(tableName)) {
				pkey = s[1];
				pkeyFlag = true;
				break;
			}
		}
		br.close();

		// calling delete row on the primary keys found
		if (pkeyFlag) {
			JSONArray content = (JSONArray) parser.parse(new FileReader(temptname + ".json"));
			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj2 = (JSONObject) content.get(i);
					pkeyList.add(obj2.get(pkey).toString());
				}
			}
		}
		
		for(String str:pkeyList){
			deleteRow(tableName, pkey, str);
		}
	}
	
	public int searchForOrSQL(String tableName, ArrayList<String> whereConds) throws IOException {
		String line;
		String pkey = "";
		JSONObject obj;
		JSONArray content2 = null;
		JSONArray content3 = new JSONArray();
		JSONParser parser = new JSONParser();
		ArrayList<String> pkeyList = new ArrayList<>();
		
		// finding pkey
		BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		while ((line = br.readLine()) != null) {
			String s[] = line.split(" ");
			if (s[0].equals(tableName)) {
				pkey = s[1];
				break;
			}
		}
		br.close();

		for (String cond : whereConds) {
			String delims = "((?<=>|<|=)|(?=>|<|=))";
			String[] currCond = cond.split(delims);
			String columnName = currCond[0].trim();
			String operator = currCond[1].trim();
			String valueToSearch = currCond[2].trim();
			if (valueToSearch.contains("'")) {
				valueToSearch = valueToSearch.replace("'", "");
			}

			JSONArray content = null;
			boolean flag = false;
			boolean operatorFlag = false;
			int checkDataType = 0;
			ArrayList<String> list = new ArrayList<>();

			if (operator.equals("=") || operator.equals("<") || operator.equals(">")) {
				operatorFlag = true;
			} else {
				System.out.println("Where operator invalid!");
				return -1;
			}

			if (!tablename.contains(tableName)) {
				System.out.println("Table does not exist!");
				return -1;
			} else {
				try {
					BufferedReader bufferedReader = new BufferedReader(new FileReader(tableName + "_meta.txt"));
					boolean columnNameFlag = false;
					while ((line = bufferedReader.readLine()) != null) {
						String s[] = line.split(" ");
						list.add(s[0]);
						if (s[0].equals(columnName)) {
							columnNameFlag = true;
							int temp = Integer.parseInt(s[1]);
							if (temp == 1) {
								flag = true;
								checkDataType = 1;
							} else if (temp == 2) {
								flag = true;
								checkDataType = 2;
							}
						}
					}
					bufferedReader.close();

					if (columnNameFlag == false) {
						System.out.println(
								"The column name: " + columnName + " does not exist in given table: " + tableName.replace("_temp", ""));
						return -1;
					} else {
						try {
							content = (JSONArray) parser.parse(new FileReader(tableName + ".json"));
						} catch (ParseException e) {
							System.out.println("ParseException in search method");
//							e.printStackTrace();
							return -1;
						}
						int len = content.size();
						if (content != null && operatorFlag == true) {
							if (flag == false) {
								if(!operator.equals("=")){
									System.out.println("Wrong operator in where clause.");
									return -1;
								}
								// handling for string data types
								for (int i = 0; i < len; i++) {
									obj = (JSONObject) content.get(i);
									if (obj.get(columnName).toString().equals(valueToSearch)) {
										if(!pkeyList.contains(obj.get(pkey))){
											pkeyList.add(obj.get(pkey).toString());
										}
									}
								}
							} else {
								// handling for Integer
								if (checkDataType == 1) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) < Integer
														.parseInt(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) > Integer
														.parseInt(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) == Integer
														.parseInt(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										}
									} catch (Exception e) {
										 System.out.println("The value: " + valueToSearch + " does not exist in this table");
										 return -1;
									}

								} else if (checkDataType == 2) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) < Float
														.parseFloat(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) > Float
														.parseFloat(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) == Float
														.parseFloat(valueToSearch)) {
													if(!pkeyList.contains(obj.get(pkey))){
														pkeyList.add(obj.get(pkey).toString());
													}
												}
											}
										}
									} catch (Exception e) {
										 System.out.println("The value: " + valueToSearch + " does not exist in this table");
										 return -1;
									}
								}
							}
						} else {
							System.out.println(
									"Unable to perform search. Please enter table name, column name and operator values correctly");
							return -1;
						}
					}

				} catch (FileNotFoundException e) {
					System.out.println("Unable to read file tablekeymeta.txt in search method");
//					e.printStackTrace();
					return -1;
				} catch (IOException io) {
					System.out.println("IOException in search method");
//					io.printStackTrace();
					return -1;
				}
			}
		}
		
		//writing the rows found to temp file
		try {
			content2 = (JSONArray) parser.parse(new FileReader(tableName + ".json"));
		} catch (ParseException e) {
			System.out.println("ParseException in search method");
//			e.printStackTrace();
			return -1;
		}
		int len = content2.size();
		if (content2 != null) {
			for (int i = 0; i < len; i++) {
				obj = (JSONObject) content2.get(i);
				if (pkeyList.contains(obj.get(pkey).toString())) {
					content3.add(obj);
				}
			}
		}
		FileWriter file = new FileWriter(tableName + ".json");
		file.write(content3.toJSONString());
		file.close();
		return 0;
	}
	
	public int searchForSQL(String tableName, ArrayList<String> whereConds) {
		
		for (String cond : whereConds) {
			String delims = "((?<=>|<|=)|(?=>|<|=))";
			String[] currCond = cond.split(delims);
			if(currCond.length > 3){
				System.out.println("Wrong condition in where clause!");
				return -1;
			}
			String columnName = currCond[0].trim();
			String operator = currCond[1].trim();
			String valueToSearch = currCond[2].trim();
			if(valueToSearch.contains("'")){
				valueToSearch = valueToSearch.replace("'", "");
			}
			
			JSONArray content = null;
			JSONArray content2 = new JSONArray();
			JSONParser parser = new JSONParser();
			JSONObject obj;
			String line = "";
			boolean flag = false;
			boolean operatorFlag = false;
			int checkDataType = 0;
			ArrayList<String> list = new ArrayList<>();

			if (operator.equals("=") || operator.equals("<") || operator.equals(">")) {
				operatorFlag = true;
			} else {
				System.out.println("Where operator invalid!");
			}

			if (!tablename.contains(tableName)) {
				System.out.println("Table does not exist!");
				return -1;
			} else {
				try {
					BufferedReader bufferedReader = new BufferedReader(new FileReader(tableName + "_meta.txt"));
					boolean columnNameFlag = false;
					while ((line = bufferedReader.readLine()) != null) {
						String s[] = line.split(" ");
						list.add(s[0]);
						if (s[0].equals(columnName)) {
							columnNameFlag = true;
							int temp = Integer.parseInt(s[1]);
							if (temp == 1) {
								flag = true;
								checkDataType = 1;
							} else if (temp == 2) {
								flag = true;
								checkDataType = 2;
							}
						}
					}
					bufferedReader.close();

					if (columnNameFlag == false) {
						tableName = tableName.replace("_temp", ""); 
						System.out.println(
								"The column name: " + columnName + " does not exist in given table: " + tableName);
						return -1;
					} else {
						try {
							content = (JSONArray) parser.parse(new FileReader(tableName + ".json"));
						} catch (ParseException e) {
							System.out.println("ParseException in search method");
							e.printStackTrace();
						}
						int len = content.size();
						if (content != null && operatorFlag == true) {
							if (flag == false) {
								if(!operator.equals("=")){
									System.out.println("Wrong operator in where clause.");
									return -1;
								}
								// handling for string data types
								for (int i = 0; i < len; i++) {
									obj = (JSONObject) content.get(i);
									if (obj.get(columnName).toString().equals(valueToSearch)) {
										content2.add(obj);
									}
								}
							} else {
								// handling for Integer
								if (checkDataType == 1) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) < Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) > Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Integer.parseInt(obj.get(columnName).toString()) == Integer
														.parseInt(valueToSearch)) {
													content2.add(obj);
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " + valueToSearch + " does not exist in this table");
									}

								} else if (checkDataType == 2) {
									try {
										if (operator.equals("<")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) < Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals(">")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) > Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										} else if (operator.equals("=")) {
											for (int i = 0; i < len; i++) {
												obj = (JSONObject) content.get(i);
												if (Float.parseFloat(obj.get(columnName).toString()) == Float
														.parseFloat(valueToSearch)) {
													content2.add(obj);
												}
											}
										}
									} catch (Exception e) {
										// System.out.println("The value: " + valueToSearch + " does not exist in this table");
									}
								}
							}
							FileWriter file = new FileWriter(tableName + ".json");
							file.write(content2.toJSONString());
							file.close();
						} else {
							System.out.println(
									"Unable to perform search. Please enter table name, column name and operator values correctly");
							return -1;
						}
					}

				} catch (FileNotFoundException e) {
					System.out.println("Unable to read file tablekeymeta.txt in search method");
//					e.printStackTrace();
					return -1;
				} catch (IOException io) {
					System.out.println("IOException in search method");
//					io.printStackTrace();
					return -1;
				}
			}
		}
		return 0;
	}

	private static void searchFromTable() {

	    Scanner scan = new Scanner(System.in);
	    System.out.println("Enter the table name: ");
	    String tableName = scan.next();
	    scan.nextLine();

	    System.out.println("Enter the attribute (column) name: ");
	    String columnName = scan.next();
	    scan.nextLine();

	    System.out.println("Enter the operator (=, <, >, <=, >=): ");
	    String operator = scan.next();
	    scan.nextLine();
	    
	    System.out.println("Enter the value: ");
	    String valueToSearch = scan.next();
	    scan.nextLine();

	    JSONParser parser = new JSONParser();
	    JSONObject obj;
	    String line = "";
	    boolean flag = false;
	    boolean operatorFlag = false;
	    int checkDataType = 0;
	    ArrayList<String> list = new ArrayList<>();

	    if (operator.equals("=") || operator.equals("<") || operator.equals(">") || operator.equals("<=") || operator.equals(">=")){
	        operatorFlag = true;
	    } else {
	        System.out.println("Operator Invalid");
	    }


	    if (!tablename.contains(tableName)) {
	        System.out.println("Table does not exist!");
	    } else {
	        try {
	            BufferedReader bufferedReader = new BufferedReader(new FileReader(tableName + "_meta.txt"));
	            boolean columnNameFlag = false;
	            while ((line = bufferedReader.readLine()) != null) {
	                String s[] = line.split(" ");
	                list.add(s[0]);
	                if (s[0].equals(columnName)) {
	                    columnNameFlag = true;
	                    int temp = Integer.parseInt(s[1]);
	                    if (temp == 1 ) {
	                        flag = true;
	                        checkDataType = 1;
	                    } else if (temp == 2){
	                        flag = true;
	                        checkDataType = 2;
	                    }
	                }
	            }
	            bufferedReader.close();

	            if (columnNameFlag == false){
	                System.out.println("The column name: " + columnName + " does not exist in given table: " + tableName);
	            } else {
	                JSONArray content = null;
	                JSONArray contentToSort = new JSONArray();
	                try {
	                    content = (JSONArray) parser.parse(new FileReader(tableName + ".json"));
	                } catch (ParseException e) {
	                    System.out.println("ParseException in search method");
	                    e.printStackTrace();
	                }
	                int len = content.size();
	                if (content != null && operatorFlag == true) {
	                    if (flag == false) {

	                        // handling for string data types
	                    	printColNames(list);
	                        for (int i = 0; i < len; i++) {
	                            obj = (JSONObject) content.get(i);
	                            if (obj.get(columnName).equals(valueToSearch)) {
//	                                System.out.println(obj.toJSONString());
	                            	printObj(list, obj);
	                            	contentToSort.add(obj);
	                            }
	                        }
	                    } else {
	                        // handling for Integer
	                        if (checkDataType == 1){
	                            try {
	                                if (operator.equals("<")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Integer.parseInt(obj.get(columnName).toString()) < Integer
	                                                .parseInt(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals(">")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Integer.parseInt(obj.get(columnName).toString()) > Integer
	                                                .parseInt(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                            printObj(list, obj);
	                                            contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals("=")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Integer.parseInt(obj.get(columnName).toString()) == Integer
	                                                .parseInt(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals("<=")){
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Integer.parseInt(obj.get(columnName).toString()) <= Integer
	                                                .parseInt(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals(">=")){
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Integer.parseInt(obj.get(columnName).toString()) >= Integer
	                                                .parseInt(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                }
	                            } catch (Exception e){
	                                //System.out.println("The value: " + valueToSearch + " does not exist in this table");
	                            }

	                        } else if (checkDataType ==2) {
	                            try {
	                                if (operator.equals("<")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Float.parseFloat(obj.get(columnName).toString()) < Float
	                                                .parseFloat(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals(">")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Float.parseFloat(obj.get(columnName).toString()) > Float
	                                                .parseFloat(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals("=")) {
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Float.parseFloat(obj.get(columnName).toString()) == Float
	                                                .parseFloat(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }

	                                    }
	                                } else if (operator.equals("<=")){
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Float.parseFloat(obj.get(columnName).toString()) <= Float
	                                                .parseFloat(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                } else if (operator.equals(">=")){
	                                	printColNames(list);
	        	                        for (int i = 0; i < len; i++) {
	                                        obj = (JSONObject) content.get(i);
	                                        if (Float.parseFloat(obj.get(columnName).toString()) >= Float
	                                                .parseFloat(valueToSearch)) {
//	                                            System.out.println(obj.toJSONString());
	                                        	printObj(list, obj);
	                                        	contentToSort.add(obj);
	                                        }
	                                    }
	                                }
	                            } catch (Exception e){
	                                //System.out.println("The value: " + valueToSearch + " does not exist in this table");
	                            }
	                        }
	                    }
	                    sortTablelist(tableName, contentToSort, list);
	                } else {
	                    System.out.println("Unable to perform search. Please enter table name, column name and operator values correctly");
	                }
	            }

	        } catch (FileNotFoundException e) {
	            System.out.println("Unable to read file tablekeymeta.txt in search method");
	            e.printStackTrace();
	        } catch (IOException io) {
	            System.out.println("IOException in search method");
	            io.printStackTrace();

	        } catch (ParseException e) {
				e.printStackTrace();
			}
	    }

	}
	
	void projectionForSQL(String tname, ArrayList<String> colname) throws IOException, ParseException{
		String name;
		ArrayList<String> list = new ArrayList<>();
		ArrayList<String> value = new ArrayList<>();
		JSONParser parser = new JSONParser();
		JSONObject obj = new JSONObject();
		boolean listflag = false;
		
		if(!tablename.contains(tname)){
			System.out.println("Table doesn't Exist");
		}else{
			if (!colname.get(0).equals("*")) {
				BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
				while ((name = br.readLine()) != null) {
					String s[] = name.split(" ");
					list.add(s[0].trim());
					value.add(s[1]);
				}
				br.close();
				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				JSONArray content2 = new JSONArray();

				int len = content.size();
				if (content != null) {
					for (int i = 0; i < len; i++) {
						JSONObject newObj = new JSONObject();
						
						obj = (JSONObject) content.get(i);

						for(String str:colname){
							newObj.put(str,obj.get(str));
						}
						
//						for (int j = 0; j < list.size(); j++) {
//							if (colname.contains(list.get(j))) {
//								newObj.put(list.get(j), obj.get(list.get(j)));
//							}
//						}
						content2.add(newObj);
					}
				}
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content2.toJSONString());
				file.close();

				BufferedWriter br2 = new BufferedWriter(new FileWriter(tname + "_meta.txt"));
				for (int i = 0; i < list.size(); i++) {
					if (colname.contains(list.get(i))) {
						br2.write(list.get(i).toString() + " " + value.get(i).toString());
						br2.newLine();
					}
				}
				br2.close();
			}

		}
	}

	private static void listfromTable() throws IOException, ParseException {
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		sc.nextLine();
		String name;
		ArrayList<String> list = new ArrayList<>();
		JSONParser parser = new JSONParser();
		JSONObject obj = new JSONObject();
		String displaylist;
		boolean listflag=false;
		ArrayList<String>colname=new ArrayList<>();

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {

			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				list.add(s[0]);
			}
			br.close();
			System.out.println("\nTable columns:");
			for (int i = 0; i < list.size(); i++) {
				System.out.print(list.get(i) + "\t");
			}
			//
			System.out.println("\n\nEnter column names to be displayed separated by spaces (all to display all columns):");
			displaylist=sc.nextLine();
			String s[] = displaylist.split(" ");
			
			if (s[0].toLowerCase().equals("all") && s.length==1) {			
				listflag = false;
			}
			else{
				listflag=true;
				for(int i=0;i<s.length;i++)
				{
					colname.add(s[i]);
				}
				
			}
			//
			for (int i = 0; i < list.size(); i++) {
				if(listflag==true){
					if(colname.contains(list.get(i))){
				System.out.print(list.get(i) + "\t\t\t");}
				}
				else{
					System.out.print(list.get(i) + "\t\t\t");
				}
			}
			System.out.println();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					for (int j = 0; j < list.size(); j++) {
						if(listflag==true ){
							if(colname.contains(list.get(j))){
						System.out.print(obj.get(list.get(j)) + "\t\t\t");}
						}
						else{
							System.out.print(obj.get(list.get(j)) + "\t\t\t");
						}
					}
					System.out.println();

				}
			}
//			if (listflag == true) {
//				sortTablelist(tname, content, colname);
//			} else {
//				sortTablelist(tname, content, list);
//			}

		}

	}
	
	public void updateSQL(String tname, ArrayList<String> setList, ArrayList<String> pkeys)
			throws IOException, ParseException {
		// getting attributes and their updated values in the lists
		ArrayList<String> attribute = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();

		for (String str : setList) {
			String delims = "((?<==)|(?==))";
			String[] currSet = str.split(delims);
			String columnName = currSet[0].trim();
			String operator = currSet[1].trim();
			String valueToUpdate = currSet[2].trim();
			if (!operator.equals("=")) {
				System.out.println("Invalid operator in set: " + str);
				break;
			}
			if (valueToUpdate.contains("'")) {
				valueToUpdate = valueToUpdate.replace("'", "");
			}
			attribute.add(columnName);
			values.add(valueToUpdate);
		}

		String name;
		String pkey = "";
		Object value = null;
		JSONParser parser = new JSONParser();
		JSONObject obj;
		JSONObject obj2;
		boolean flag = false, loop = false;

		BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
		while ((name = br.readLine()) != null) {
			String s[] = name.split(" ");
			if (s[0].equals(tname)) {
				pkey = s[1];
				flag = true;
			}
		}
		br.close();
		JSONArray list = new JSONArray();
		JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
		int len = content.size();
		if (content != null && flag == true) {
			for (int i = 0; i < len; i++) {
				obj = (JSONObject) content.get(i);
				obj2 = obj;
				if (pkeys.contains(obj.get(pkey).toString())) {
					JSONObject newObj = new JSONObject();
					newObj.put(pkey, obj.get(pkey).toString());
					BufferedReader brmeta = new BufferedReader(new FileReader(tname + "_meta.txt"));
					while ((name = brmeta.readLine()) != null) {
						String s[] = name.split(" ");
						if (!s[0].equals(pkey)) {
							// check if present in attribute
							if (attribute.contains(s[0])) {
								int index = attribute.indexOf(s[0]);
								if (Integer.parseInt(s[1]) == 1) {
									try {
										value = Integer.parseInt(values.get(index));
									} catch (Exception e) {
										System.out.println("Invalid value: " + values.get(index));
									}
								} else if (Integer.parseInt(s[1]) == 2) {
									try {
										value = Float.parseFloat(values.get(index));
									} catch (Exception e) {
										System.out.println("Invalid value: " + values.get(index));
									}
								} else if (Integer.parseInt(s[1]) == 5) {
									try {
										value = Boolean.parseBoolean(values.get(index));
									} catch (Exception e) {
										System.out.println("Invalid value: " + values.get(index));
									}
								} else if (Integer.parseInt(s[1]) == 4) {
									DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
									Date date = null;
									try {
										value = values.get(index);
										date = format.parse(value.toString());
									} catch (Exception e) {
										System.out.println("Invalid value: " + values.get(index));
									}
								} else {
									value = values.get(index);
								}
							} else {
								value = obj2.get(s[0]);
							}
							newObj.put(s[0], value);
						}
						obj = newObj;
					}
					brmeta.close();
				}
				list.add(obj);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(list.toJSONString());
				file.close();
			}
		}
	}

	private static void updatefromTable() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		sc.nextLine();
		String name;
		String pkey = "";
		Object value = null;
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag = false, loop = false, loop2 = false;
		long recid;
		BTree tree = null;
		int prim = 0;
		int location = Integer.MAX_VALUE;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {

			System.out.println("Enter Primary key Value: ");
			loop2 = true;
			while (loop2) {
				try {
					prim = sc.nextInt();
					sc.nextLine();
					loop2 = false;
				} catch (InputMismatchException e) {
					System.out.println("Invalid value!");
					sc.nextLine();
				}
			}
			// comented due to b+ tree
			// BufferedReader br = new BufferedReader(new
			// FileReader("tablekeymeta.txt"));
			// while ((name = br.readLine()) != null) {
			// String s[] = name.split(" ");
			// if (s[0].equals(tname)) {
			// pkey = s[1];
			// flag = true;
			// }
			// }
			// br.close();
			// addition due to b+ tree
			pkey = tname + "_pkey";
			//
			recid = recman.getNamedObject(tname + "_btree");
			if (recid != 0) {
				tree = BTree.load(recman, recid);
			}

			Object loc = tree.find(prim);
			if (loc != null)
				location = (int) tree.find(prim);

			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && recid != 0 && location < len) {
				// for (int i = 0; i < len; i++) {

				obj = (JSONObject) content.get(location);
				content.remove(location);
				// if (obj.get(pkey).toString().equals(prim)) {
				JSONObject newObj = new JSONObject();
				newObj.put(pkey, prim);
				BufferedReader brmeta = new BufferedReader(new FileReader(tname + "_meta.txt"));
				while ((name = brmeta.readLine()) != null) {
					String s[] = name.split(" ");
					if (!s[0].equals(pkey)) {
						System.out.println("Enter value for " + s[0]);
						//
						if (Integer.parseInt(s[1]) == 1) {
							loop = true;
							while (loop) {
								try {
									value = sc.nextInt();
									sc.nextLine();
									loop = false;
								} catch (InputMismatchException e) {
									System.out.println("Invalid value!");
									sc.nextLine();
								}
							}
						} else if (Integer.parseInt(s[1]) == 2) {
							loop = true;
							while (loop) {
								try {
									value = sc.nextFloat();
									sc.nextLine();
									loop = false;
								} catch (InputMismatchException e) {
									System.out.println("Invalid value!");
									sc.nextLine();
								}
							}
						} else if (Integer.parseInt(s[1]) == 5) {
							loop = true;
							while (loop) {
								try {
									value = sc.nextBoolean();
									sc.nextLine();
									loop = false;
								} catch (InputMismatchException e) {
									System.out.println("Invalid value!");
									sc.nextLine();
								}
							}
						} else if (Integer.parseInt(s[1]) == 4) {
							DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
							loop = true;
							Date date = null;
							while (loop) {
								try {
									value = sc.next();
									date = format.parse(value.toString());
									sc.nextLine();
									loop = false;
								} catch (InputMismatchException e) {
									System.out.println("Invalid value!");
									sc.nextLine();
								} catch (java.text.ParseException e) {
									System.out.println("Invalid value!");
									sc.nextLine();
								}
							}
						} else {
							value = sc.nextLine();
						}
						//

						// value=sc.next();
						newObj.put(s[0], value);

					}
					obj = newObj;

				}
				brmeta.close();

				// }
				content.add(location, obj);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				recreateIndexOnCol(tname);

				// }
			}
		}
	}

//	public static int updateForSQLAndS(String tname, ArrayList<String> setList, ArrayList<String> whereConds) throws FileNotFoundException, IOException, ParseException{
//		
//		HashMap<String, String> whereopr = new HashMap<>();
//		HashMap<String, String> whereval = new HashMap<>();
//		HashMap<String, String> setopr = new HashMap<>();
//		HashMap<String, String> setval = new HashMap<>();
//		JSONParser parser = new JSONParser();
//		JSONArray list=new JSONArray();
//		JSONObject obj= new JSONObject();
//		boolean flag=true;
//		for (String cond : whereConds) {
//			String delims = "((?<=>|<|=)|(?=>|<|=))";
//			String[] currCond = cond.split(delims);
//			if(currCond.length > 3){
//				System.out.println("Wrong condition in where clause!");
//				return -1;
//			}
//			String columnName = currCond[0].trim();
//			String operator = currCond[1].trim();
//			String valueToSearch = currCond[2].trim();
//			if(valueToSearch.contains("'")){
//				valueToSearch = valueToSearch.replace("'", "");
//			whereopr.put(columnName, operator);
//			whereval.put(columnName, valueToSearch);
//			}
//		}
//		
//		for (String str : setList) {
//			String delims = "((?<==)|(?==))";
//			String[] currSet = str.split(delims);
//			String columnName = currSet[0].trim();
//			String operator = currSet[1].trim();
//			String valueToUpdate = currSet[2].trim();
//			if (!operator.equals("=")) {
//				System.out.println("Invalid operator in set: " + str);
//				break;
//			}
//			if (valueToUpdate.contains("'")) {
//				valueToUpdate = valueToUpdate.replace("'", "");
//			}
//			setopr.put(columnName, operator);
//			setval.put(columnName, valueToUpdate);
//		}
//		JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
//		for(int i=0;i<content.size();i++)
//		{	flag=true;
//			obj=(JSONObject) content.get(i);
//			for(Map.Entry<String, String> entry : whereopr.entrySet()){
//	            if(entry.getValue().equals("="))
//	            {
//	            	if(obj.get(entry.getKey()).toString().equals(whereval.get(entry.getKey()).toString()))
//	            	{
//	            		
//	            	}
//	            	else
//	            		flag=false;
//	            }
//	            if(entry.getValue().equals("<"))
//	            {
//	            	String val;
//	            	Object o;
//	            	val=obj.get(entry.getKey()).toString();
//	            	String v;
//	            	Object ob; 
//	            	v=whereval.get(entry.getKey()).toString();
//	            	try{
//	            		if(Float.parseFloat(val) >= Float.parseFloat(v))
//	            			flag=false;
//	            	}
//	            	catch(Exception e)
//	            	{
//	            		if(Integer.parseInt(val)>=Integer.parseInt(v))
//	            			flag=false;
//	            	}
//	            		
//	            		
//	            }
//	            if(entry.getValue().equals(">"))
//	            {
//	            	String val;
//	            	Object o;
//	            	val=obj.get(entry.getKey()).toString();
//	            	String v;
//	            	Object ob; 
//	            	v=whereval.get(entry.getKey()).toString();
//	            	try{
//	            		if(Float.parseFloat(val) <= Float.parseFloat(v))
//	            			flag=false;
//	            	}
//	            	catch(Exception e)
//	            	{
//	            		if(Integer.parseInt(val)<=Integer.parseInt(v))
//	            			flag=false;
//	            	}
//	            		
//	            		
//	            }
//	            
//	            
//	        }
//			if(flag==true)
//			{
//			
//				for(Map.Entry<String, String> entry : setval.entrySet()){
//					
//					obj.put(entry.getKey(), entry.getValue());
//				}
//			}
//			content.remove(i);
//			content.add(i, obj);
//		}
//		
//		
//	}
			
	private static void deletefromTable() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		sc.nextLine();
		// String name;
		// String pkey = "";
		JSONParser parser = new JSONParser();
		// JSONObject obj;
		// boolean flag = false;
		long recid;
		BTree tree = null;
		int location = Integer.MAX_VALUE;
		Tuple tuple = new Tuple();
		TupleBrowser browser;
		boolean loop2 = false;
		int prim = Integer.MAX_VALUE;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {
			System.out.println("Enter Primary key Value: ");
			loop2 = true;
			while (loop2) {
				try {
					prim = sc.nextInt();
					sc.nextLine();
					loop2 = false;
				} catch (InputMismatchException e) {
					System.out.println("Invalid value!");
					sc.nextLine();
				}
			}
			// BufferedReader br = new BufferedReader(new
			// FileReader("tablekeymeta.txt"));
			// while ((name = br.readLine()) != null) {
			// String s[] = name.split(" ");
			// if (s[0].equals(tname)) {
			// pkey = s[1];
			// flag = true;
			// }
			// }
			// br.close();
			// pkey=tname+"_pkey";
			recid = recman.getNamedObject(tname + "_btree");
			if (recid != 0)
				tree = BTree.load(recman, recid);

			Object loc = tree.find(prim);
			if (loc != null)
				location = (int) tree.find(prim);

			if (recid != 0) {
				// JSONArray list = new JSONArray();
				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				if (content != null && location < len) {
					// for (int i = 0; i < len; i++) {
					// obj = (JSONObject) content.get(location);
					// if (!obj.get(pkey).toString().equals(prim)) {
					// list.add(obj);
					// }
					// }
					content.remove(location);

					FileWriter file = new FileWriter(tname + ".json");
					file.write("");
					file.write(content.toJSONString());
					file.close();

					// experimental
					browser = tree.browse();
					while (browser.getNext(tuple)) {
						// System.out.println( tuple.getKey()+"
						// "+tuple.getValue());
						if ((int) tuple.getKey() > prim) {
							tree.insert(tuple.getKey(), (int) tuple.getValue() - 1, true);
						}
					}
					tree.remove(prim);
					recman.commit();
					//

					System.out.println("\n1 record deleted.");
					recreateIndexOnCol(tname);
				}
			} else {
				System.out.println("\nPrimary key not found. Returning to main menu.");
			}
		}
	}
	
	void insertSQL(String tname, ArrayList<String> attributeList, ArrayList<String> valueList)
			throws IOException, ParseException {

		String name;
		String pkey = null;
		String keyline;
		Object value = null;
		JSONObject obj;
		long recid;
		BTree tree;
		JSONParser parser = new JSONParser();
		org.json.simple.JSONObject newObj = new org.json.simple.JSONObject();
//		ArrayList<String> list = new ArrayList<>();
//		boolean flag = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			BufferedReader filekey = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((keyline = filekey.readLine()) != null) {
				String s[] = keyline.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
//					flag = true;
				}
			}
			filekey.close();
			
			// read content for unique key value
			filekey = new BufferedReader(new FileReader(tname + "_unique.txt"));
			keyline = filekey.readLine();
			String sunique[] = keyline.split(" ");
			Integer uniquekey = Integer.parseInt(sunique[0]);
			filekey.close();

			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
//			if (content != null && flag == true) {
//				for (int i = 0; i < len; i++) {
//					obj = (JSONObject) content.get(i);
//					list.add(obj.get(pkey).toString());
//				}
//			}

			// checks on attribute list and values
			BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));
			while ((name = br2.readLine()) != null) {
				String s[] = name.split(" ");
				if (!s[0].equals(tname + "_pkey")) {
					if (!attributeList.contains(s[0])) {
						System.out.println("Attribute missing. Wrong insert SQL!");
						return;
					}
				}
			}
			br2.close();
			if (attributeList.size() != valueList.size()) {
				System.out.println("Value missing. Wrong insert SQL!");
			}

			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			br.readLine();
			newObj.put(tname+"_pkey", uniquekey);
			uniquekey++;
			
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				int index = attributeList.indexOf(s[0]);

				if (Integer.parseInt(s[1]) == 1) {
					try {
						value = Integer.parseInt(valueList.get(index));
					} catch (Exception e) {
						System.out.println("Invalid value for: " + attributeList.get(index));
						break;
					}
				} else if (Integer.parseInt(s[1]) == 2) {
					try {
						value = Float.parseFloat(valueList.get(index));
					} catch (Exception e) {
						System.out.println("Invalid value for: " + attributeList.get(index));
						break;
					}
				} else if (Integer.parseInt(s[1]) == 5) {
					try {
						value = Boolean.parseBoolean(valueList.get(index));
					} catch (Exception e) {
						System.out.println("Invalid value for: " + attributeList.get(index));
						break;
					}
				} else if (Integer.parseInt(s[1]) == 4) {
					DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
					Date date = null;
					try {
						value = valueList.get(index);
						date = format.parse(value.toString());
					} catch (Exception e) {
						System.out.println("Invalid value for: " + attributeList.get(index));
						break;
					}
				} else {
					value = valueList.get(index);
				}
				newObj.put(s[0], value);
			}
			br.close();

			// writing to the table's json file
			content.add(newObj);
			FileWriter file = new FileWriter(tname + ".json");
			file.write("");
			file.write(content.toJSONString());
			file.close();
			System.out.println("\nRecord added successfully.");

			// write file content to unique file
			FileWriter writerunique = new FileWriter(tname + "_unique.txt");
			writerunique.write("");
			writerunique.write(uniquekey.toString());
			writerunique.close();

			// writing to b tree
			recid = recman.getNamedObject(tname + "_btree");
			if (recid != 0) {
				tree = BTree.load(recman, recid);
				tree.insert(uniquekey - 1, content.size() - 1, false);
				recman.commit();
			} else {
				System.out.println("Adding index failed");
			}
			//
            //adding index on columns
            br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			br.readLine();
			
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				insertToColBTree(tname,s[0],newObj.get(s[0]).toString(),content.size()-1,s[1]);
			}
            //
		}
	}

	private static void inserttoTable() throws IOException, ParseException {
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		sc.nextLine();
		String name;
		String pkey = null;
		String keyline;
		Object value = null;
		JSONObject obj;
		JSONParser parser = new JSONParser();
		org.json.simple.JSONObject newObj = new org.json.simple.JSONObject();
		boolean loop = true;
		ArrayList<String> list = new ArrayList<>();
		boolean flag = false;
		String choice;
		long recid;
		BTree tree;
		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			do{
			BufferedReader filekey = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((keyline = filekey.readLine()) != null) {
				String s[] = keyline.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
					flag = true;
				}
			}
			filekey.close();
			//read content for unique key value
			filekey = new BufferedReader(new FileReader(tname+"_unique.txt"));
			keyline = filekey.readLine();
				String sunique[] = keyline.split(" ");
				Integer uniquekey=Integer.parseInt(sunique[0]); 								
			filekey.close();
			
			//
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && flag == true) {
//				for (int i = 0; i < len; i++) {
//					obj = (JSONObject) content.get(i);
//					list.add(obj.get(pkey).toString());
//				}
			}
			
			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			br.readLine();
			newObj.put(tname+"_pkey", uniquekey);
			uniquekey++;
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				System.out.println("Enter value for " + s[0]);
				// type check start
				if (Integer.parseInt(s[1]) == 1) {
					loop = true;
					while (loop) {
						try {
							value = sc.nextInt();
							sc.nextLine();
							loop = false;
						} catch (InputMismatchException e) {
							System.out.println("Invalid value!");
							sc.nextLine();
						}
					}
				}
				else if (Integer.parseInt(s[1]) == 2) {
					loop = true;
					while (loop) {
						try {
							value = sc.nextFloat();
							sc.nextLine();
							loop = false;
						} catch (InputMismatchException e) {
							System.out.println("Invalid value!");
							sc.nextLine();
						}
					}
				}
				else if (Integer.parseInt(s[1]) == 5) {
					loop = true;
					while (loop) {
						try {
							value = sc.nextBoolean();
							sc.nextLine();
							loop = false;
						} catch (InputMismatchException e) {
							System.out.println("Invalid value!");
							sc.nextLine();
						}
					}
				} 
				else if (Integer.parseInt(s[1]) == 4) {
					DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
					loop = true;
					Date date = null;
					while (loop) {
						try {
							value = sc.next();
							date = format.parse(value.toString());
							sc.nextLine();
							loop = false;
						} catch (InputMismatchException |java.text.ParseException e) {
							System.out.println("Invalid value!");
							sc.nextLine();
						} 
					}
				} 
				else {
					value = sc.nextLine();
				}
				// type check ends
				newObj.put(s[0], value);
			}
			br.close();
			
//			if (content.size()!=0 && list.contains(newObj.get(pkey).toString())) {
//				System.out.println("Primary key already exists. Record cannot be added.");
//			} else {
				content.add(newObj);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				System.out.println("\nRecord added successfully.");
			//}
			//write file content to unique file
			FileWriter writerunique = new FileWriter(tname+"_unique.txt");
			writerunique.write("");
			writerunique.write(uniquekey.toString());
			writerunique.close();
			
			//
			// writing to b tree
			recid = recman.getNamedObject( tname+"_btree" );
            if ( recid != 0 ) {
                tree = BTree.load( recman, recid );
                tree.insert( uniquekey-1, content.size()-1, false );
                recman.commit();
            }
            else{
            	System.out.println("Adding index failed");
            }
			//
            //adding index on columns
            br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			br.readLine();
			
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				insertToColBTree(tname,s[0],newObj.get(s[0]).toString(),content.size()-1,s[1]);
			}
            //
            
			System.out.println("Do you want to add another record? (y/n): ");
			choice=sc.next();
			sc.nextLine();
			}while(choice.toLowerCase().equals("y"));
		}
	}
	
	private static void insertToColBTree(String tname,String colname,String key, Integer ind, String type) throws IOException
	{
		long recid;
		BTree tree;
		Object obj;
		String forapp;
		Integer t=Integer.parseInt(type);
		recid = recman.getNamedObject( tname+"_"+colname+"_btree" );
        if ( recid != 0 ) {
            tree = BTree.load( recman, recid );
            if(t==1)
            	obj=tree.find(Integer.parseInt(key));
            else
            obj=tree.find(key);
            if(obj==null)
			{
            	if(t==1)
            	tree.insert(Integer.parseInt(key), ind, false);	
            	else	
				tree.insert(key, ind, false);
			}
			else{
				forapp=(String)obj.toString();
				forapp=forapp+","+ind.toString();
				if(t==1)
				tree.insert(Integer.parseInt(key), forapp, true);
				else
				tree.insert(key, forapp, true);
				
			}
            
            recman.commit();
        }
	}
	private static void deleteFromColBTree(String tname,String colname,String key, Integer ind) throws IOException
	{
		long recid;
		BTree tree;
		Object obj;
		String forapp;
		ArrayList<String> al=new ArrayList<>();
		recid = recman.getNamedObject( tname+"_"+colname+"_btree" );
        if ( recid != 0 ) {
            tree = BTree.load( recman, recid );
            obj=tree.find(key);
			
				forapp=(String)obj.toString();
				String s[]=forapp.split(",");
				
				if(s.length<2)
					tree.remove(key);
				else{
					for(int i=0;i<s.length;i++)
						al.add(s[i]);
					al.remove(ind.toString());
					forapp=al.get(0);
					for(int i=1;i<al.size();i++)
						forapp=forapp+","+al.get(i);
					tree.insert(key, forapp, true);
				}	
            
            recman.commit();
        }
	}

	private static void listTables() {
		System.out.println("\nTables in the database:\n");
		for (String str : tablename) {
			System.out.println(str);
		}
	}

	private static void deleteTable() throws IOException {
		System.out.println("Enter table name to be deleted: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		String keyline;
		long recid;
		if (!tablename.contains(tname))
			System.out.println("Table does not exist!");
		else {
			recid = recman.getNamedObject( tname+"_btree" );
			if(recid!=0)recman.delete(recid);
			BufferedReader filekey = new BufferedReader(new FileReader(tname+"_meta.txt"));
			filekey.readLine();
			while ((keyline = filekey.readLine()) != null) {
				String s[] = keyline.split(" ");
				recid = recman.getNamedObject( tname+"_"+s[0]+"_btree" );
				if(recid!=0)recman.delete(recid);
				}
			filekey.close();
			tablename.remove(tname);
			tablekey.remove(tname);
			File file = new File(tname + "_meta.txt");
			if (file.exists())
				file.delete();
			file = new File(tname + ".json");
			if (file.exists())
				file.delete();
			file = new File(tname + "_unique.txt");
			if (file.exists())
				file.delete();
			
			writetoTableList();
			writetoKeyMeta();
			System.out.println("\nTable deleted Successfully ");
			recman.commit();
		}
	}
	
	void deleteTempTable(String temptname) throws IOException {
		// delete temp table if already present
		if (tablename.contains(temptname)) {
			tablename.remove(temptname);
			tablekey.remove(temptname);
			File file = new File(temptname + "_meta.txt");
			if (file.exists()) {
				file.delete();
			}
			file = new File(temptname + ".json");
			if (file.exists()) {
				file.delete();
			}
			writetoTableList();
			writetoKeyMeta();
		}
	}
	
	static void createTempTable(String tname, String temptname) throws IOException {
		String keyline;
		String pkey = "";

		BufferedReader filekey = new BufferedReader(new FileReader("tablekeymeta.txt"));
		while ((keyline = filekey.readLine()) != null) {
			String s[] = keyline.split(" ");
			if (s[0].equals(tname)) {
				pkey = s[1];
			}
		}
		filekey.close();
		
		//delete temp table if already present
		if (tablename.contains(temptname)) {
			tablename.remove(temptname);
			tablekey.remove(temptname);
			File file = new File(temptname + "_meta.txt");
			if (file.exists()) {
				file.delete();
			}
			file = new File(temptname + ".json");
			if (file.exists()) {
				file.delete();
			}
			writetoTableList();
			writetoKeyMeta();
		}

		// copy meta file
		File input = new File(tname + "_meta.txt");
		File output = new File(temptname + "_meta.txt");
		if (!output.exists()) {
			output.createNewFile();
		}
		copyFileUsingFileStreams(input, output);

		// copy json file
		input = new File(tname + ".json");
		output = new File(temptname + ".json");
		if (!output.exists()) {
			output.createNewFile();
		}
		copyFileUsingFileStreams(input, output);

		tablekey.put(temptname, pkey);
		tablename.add(temptname);

		writetoTableList();
		writetoKeyMeta();
	}
	
	private static void copyFileUsingFileStreams(File source, File dest)
			throws IOException {
		InputStream input = null;
		OutputStream output = null;
		try {
			input = new FileInputStream(source);
			output = new FileOutputStream(dest);
			byte[] buf = new byte[1024];
			int bytesRead;
			while ((bytesRead = input.read(buf)) > 0) {
				output.write(buf, 0, bytesRead);
			}
		} finally {
			input.close();
			output.close();
		}
	}

	private static void createTable() throws IOException {
		Scanner sc = new Scanner(System.in);
		String dt;
		System.out.println("Enter the table name:");
		String tname = sc.next();
		sc.nextLine();
		ArrayList<String> types = new ArrayList<String>();
		types.add("1");
		types.add("2");
		types.add("3");
		types.add("4");
		types.add("5");
		ArrayList<String> colNames = new ArrayList<String>();
		int slen=0;
		int count=0;
		boolean loop=true;
		long recid;
		String Btreename = tname+"_btree";
		BTree tree;
		if (tablename.contains(tname))
			System.out.println("Table already exists!");
		else {
			BufferedWriter br = new BufferedWriter(new FileWriter(tname + "_meta.txt"));
			System.out.println("Enter the number of columns in the table:");
			loop=true;
			while(loop){
			try{
			count = sc.nextInt();
			loop=false;
			}
			catch(java.util.InputMismatchException e)
			{
				System.out.println("Input type can only be Integer, Please enter again");
				sc.nextLine();
			}
			}
			sc.reset();
			System.out.println("Available data types: Integer(1) Float(2) String(3) Date(4) Boolean(5)");
			br.write(tname+"_pkey 1");
			br.newLine();
			for (int i = 0; i < count; i++) {
				String line;
				sc.nextLine();
				do{
				System.out.println("Enter Column Name");
				line = sc.nextLine();
					String s[] = line.split(" ");
					slen=s.length;
				}while(slen>1);
				line=line.trim();
				colNames.add(line);
				do {
					System.out.println("Enter Datatype Value e.g 1 for Integer : ");
					dt = sc.next().trim();
				} while (!types.contains(dt));
				br.write(line + " " + dt);
				br.newLine();
			}
			br.close();
			File file = new File(tname + ".json");
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(tname + ".json");
			fw.write("[]");
			fw.close();
			String primkey;
			sc.nextLine();
//			do {
//				System.out.println("What is the primary key for this Table: ");
//				primkey = sc.nextLine();
//			} while (!colNames.contains(primkey));
			primkey=tname+"_pkey";
			tablekey.put(tname, primkey);
			tablename.add(tname);
			System.out.println("New Table " + tname + " Created. ");
			///new file for unique
			
			File fileunique = new File(tname + ".json");
			if (!fileunique.exists()) {
				fileunique.createNewFile();
			}
			FileWriter funique = new FileWriter(tname + "_unique.txt");
			funique.write("0");
			funique.close();
			
			tree = BTree.createInstance( recman, new IntegerComparator() );
			recman.setNamedObject( Btreename, tree.getRecid() );
			
			writetoTableList();
			writetoKeyMeta();
			recman.commit();
		}
	}
}
