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
import java.util.Map;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class entryClass {

	static ArrayList<String> tablename;
	static HashMap<String, String> tablekey = new HashMap<String, String>();

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

		menu();
		
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
			System.out.print(list.get(i) + "\t\t\t\t");
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
			System.out.println("8)Search Record for an Attribute\n9)Sort Table\n10)Print B+ tree\n11)Enter SQL command\n12)Exit");
			System.out.print("\nEnter your choice : ");
			Scanner scan = new Scanner(System.in);
			option = scan.next();

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
				break;
			case "11": 
				String inSQL;
				System.out.print("\nEnter the SQL statement : ");
				scan.nextLine();
				inSQL = scan.nextLine();
				parser parse = new parser();
				parse.parseSQL(inSQL);
				break;
			case "12":
				break;
			default:
				System.out.println("Invalid Entry");
				break;

			}
			if(!option.equals("12")){
			System.out.println("\nPress ENTER to Continue");
			System.in.read();}

		} while (!option.equals("12"));
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
					for (int k = 0; k < len; k++) {
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
//					printObj(list, obj);
					content2.add(obj);
				}
				FileWriter file = new FileWriter(tname + ".json");
				file.write(content2.toJSONString());
				file.close();
			}
		}
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

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {

			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					if (!obj.get(pkey).toString().equals(pkeyVal)) {
						list.add(obj);
					}
				}
			}
			FileWriter file = new FileWriter(tname + ".json");
			file.write("");
			file.write(list.toJSONString());
			file.close();
			System.out.println("\n1 record deleted.");
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
									if (obj.get(columnName).equals(valueToSearch)) {
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
	
	public void searchForSQL(String tableName, ArrayList<String> whereConds) {
		
		for (String cond : whereConds) {
			String delims = "((?<=>|<|=)|(?=>|<|=))";
			String[] currCond = cond.split(delims);
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
								"The column name: " + columnName + " does not exist in given table: " + tableName);
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

								// handling for string data types
								for (int i = 0; i < len; i++) {
									obj = (JSONObject) content.get(i);
									if (obj.get(columnName).equals(valueToSearch)) {
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
			if (listflag == true) {
				sortTablelist(tname, content, colname);
			} else {
				sortTablelist(tname, content, list);
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
		boolean flag = false, loop = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {
			System.out.println("Enter Primary key Value: ");
			String prim = sc.next();
			sc.nextLine();
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
					if (obj.get(pkey).toString().equals(prim)) {
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
										} catch (InputMismatchException e) {
											System.out.println("Invalid value!");
											sc.nextLine();
										} catch (java.text.ParseException e) {
											System.out.println("Invalid value!");
											sc.nextLine();
										}
									}
								} 
								else {
									value = sc.nextLine();
								}
								//

								// value=sc.next();
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
	}

	private static void deletefromTable() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		sc.nextLine();
		String name;
		String pkey = "";
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't exist!");
		} else {
			System.out.println("Enter Primary key Value: ");
			String prim = sc.next();
			BufferedReader br = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
					flag = true;
				}
			}
			br.close();

			if(flag == true){
			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					if (!obj.get(pkey).toString().equals(prim)) {
						list.add(obj);
					}
				}
			}
			FileWriter file = new FileWriter(tname + ".json");
			file.write("");
			file.write(list.toJSONString());
			file.close();
			System.out.println("\n1 record deleted.");
			}else{
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
		JSONParser parser = new JSONParser();
		org.json.simple.JSONObject newObj = new org.json.simple.JSONObject();
		ArrayList<String> list = new ArrayList<>();
		boolean flag = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {
			BufferedReader filekey = new BufferedReader(new FileReader("tablekeymeta.txt"));
			while ((keyline = filekey.readLine()) != null) {
				String s[] = keyline.split(" ");
				if (s[0].equals(tname)) {
					pkey = s[1];
					flag = true;
				}
			}
			filekey.close();

			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && flag == true) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					list.add(obj.get(pkey).toString());
				}
			}

			// checks on attribute list and values
			BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));
			while ((name = br2.readLine()) != null) {
				String s[] = name.split(" ");
				if (!attributeList.contains(s[0])) {
					System.out.println("Attribute missing. Wrong insert SQL!");
				}
			}
			br2.close();
			if (attributeList.size() != valueList.size()) {
				System.out.println("Value missing. Wrong insert SQL!");
			}

			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
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

			if (content.size() != 0 && list.contains(newObj.get(pkey).toString())) {
				System.out.println("Primary key already exists. Record cannot be added.");
			} else {
				content.add(newObj);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				System.out.println("\nRecord added successfully.");
			}
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
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					list.add(obj.get(pkey).toString());
				}
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
			
			if (content.size()!=0 && list.contains(newObj.get(pkey).toString())) {
				System.out.println("Primary key already exists. Record cannot be added.");
			} else {
				content.add(newObj);
				FileWriter file = new FileWriter(tname + ".json");
				file.write("");
				file.write(content.toJSONString());
				file.close();
				System.out.println("\nRecord added successfully.");
			}
			//write file content to unique file
			FileWriter writerunique = new FileWriter(tname+"_unique.txt");
			writerunique.write("");
			writerunique.write(uniquekey.toString());
			writerunique.close();
			
			//
			System.out.println("Do you want to add another record? (y/n): ");
			choice=sc.next();
			sc.nextLine();
			}while(choice.toLowerCase().equals("y"));
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
		if (!tablename.contains(tname))
			System.out.println("Table does not exist!");
		else {
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
		}
	}
	
	void createTempTable(String tname, String temptname) throws IOException {
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
			
			writetoTableList();
			writetoKeyMeta();
		}
	}
}
