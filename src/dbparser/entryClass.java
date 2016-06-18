package dbparser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

		// createTable();
		// createTable();
		// deleteTable();
		// listTables();
		// inserttoTable();
		// deletefromTable();
		// updatefromTable();
		// listfromTable();
		// searchFromTable();
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

	private static void menu() throws IOException, ParseException {

		String option;
		do {
			System.out.println("Movie Database System ");
			System.out.println("MENU ");
			System.out.println("1)Create New Table\n2)Delete Existing Table\n3)List all tables in Database");
			System.out.println(
					"4)Insert Record into a Table\n5)Delete Record from a Table\n6)Update Record from a Table\n7)List content of a Table");
			System.out.println("8)Search Record for an Attribute\n9)Sort Table\n10)Exit");
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
			default:
				System.out.println("Invalid Entry");
				break;

			}
			if(!option.equals("10")){
			System.out.println("\n\nPress ENTER to Continue");
			System.in.read();}

		} while (!option.equals("10"));
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
				ArrayList<String> sortedList = new ArrayList<>();
				JSONParser parser = new JSONParser();
				JSONObject obj = new JSONObject();
				BufferedReader br2 = new BufferedReader(new FileReader(tname + "_meta.txt"));

				// get the fields in the table into list
				while ((name = br2.readLine()) != null) {
					String s[] = name.split(" ");
					list.add(s[0]);
				}
				br2.close();

				JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
				int len = content.size();
				if (content != null) {
					for (int i = 0; i < len; i++) {
						obj = (JSONObject) content.get(i);
						sortedList.add(obj.get(pkey).toString());
					}
					System.out.println("Enter the following for displaying the contents sorted based on primary key:");
					while (true) {
						System.out.println("1 for ascending order, 2 for descending order: ");
						String choice = sc.next();

						if (choice.equals("1")) {
							Collections.sort(sortedList);
							break;
						} else if (choice.equals("2")) {
							Collections.sort(sortedList, Collections.reverseOrder());
							break;
						} else {
							System.out.println("Enter either 1 or 2!");
						}
					}

					for (int i = 0; i < list.size(); i++) {
						System.out.print(list.get(i) + "\t\t\t\t\t");
					}
					System.out.println();

					for (int i = 0; i < len; i++) {
						for (int k = 0; k < len; k++) {
							obj = (JSONObject) content.get(k);
							if (obj.get(pkey) == sortedList.get(i)) {
								flag2 = true;
								break;
							}
						}
						if (flag2) {
							for (int j = 0; j < list.size(); j++) {
								System.out.print(obj.get(list.get(j)) + "\t\t\t");
							}
							System.out.println();
						} else {
							System.out.print("\t\t\tSomething wrong with the database!!");
						}
					}
				}
			}
		}
		// sc.close();
	}

	private static void searchFromTable() {

		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the table name: ");
		String tableName = scan.next();

		System.out.println("Enter the attribute name: ");
		String columnName = scan.next();

		System.out.println("Enter the operator: ");
		String operator = scan.next();

		System.out.println("Enter the attribute value: ");
		String valueToSearch = scan.next();

		JSONParser parser = new JSONParser();
		JSONObject obj;
		String line = "";
		boolean flag = false;

		if (!tablename.contains(tableName)) {
			System.out.println("Table does not exist!");
		} else {
			try {

				BufferedReader bufferedReader = new BufferedReader(new FileReader(tableName + "_meta.txt"));
				while ((line = bufferedReader.readLine()) != null) {
					String s[] = line.split(" ");
					if (s[0].equals(columnName)) {
						int temp = Integer.parseInt(s[1]);
						if (temp == 1 || temp == 2) {
							flag = true;
						}
					}
				}
				bufferedReader.close();

				JSONArray content = null;
				try {
					content = (JSONArray) parser.parse(new FileReader(tableName + ".json"));
				} catch (ParseException e) {
					System.out.println("ParseException in search method");
					e.printStackTrace();
				}
				int len = content.size();
				if (content != null) {
					if (flag == false) {

						// handling for string data types

						for (int i = 0; i < len; i++) {

							obj = (JSONObject) content.get(i);
							if (obj.get(columnName).equals(valueToSearch)) {
								System.out.println(obj.toJSONString());

							}
						}
					} else {
						// handling for Integer and Float data types

						if (operator.equals("<")) {
							for (int i = 0; i < len; i++) {

								obj = (JSONObject) content.get(i);
								if (Integer.parseInt(obj.get(columnName).toString()) < Integer
										.parseInt(valueToSearch)) {
									System.out.println(obj.toJSONString());

								}
							}
						} else if (operator.equals(">")) {
							for (int i = 0; i < len; i++) {

								obj = (JSONObject) content.get(i);
								if (Integer.parseInt(obj.get(columnName).toString()) > Integer
										.parseInt(valueToSearch)) {
									System.out.println(obj.toJSONString());

								}
							}
						} else if (operator.equals("=")) {
							for (int i = 0; i < len; i++) {

								obj = (JSONObject) content.get(i);
								if (Integer.parseInt(obj.get(columnName).toString()) == Integer
										.parseInt(valueToSearch)) {
									System.out.println(obj.toJSONString());

								}
							}
						}

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

	private static void listfromTable() throws IOException, ParseException {
		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		String name;
		ArrayList<String> list = new ArrayList<>();
		JSONParser parser = new JSONParser();
		JSONObject obj = new JSONObject();

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
		} else {

			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				list.add(s[0]);
			}
			br.close();
			for (int i = 0; i < list.size(); i++) {
				System.out.print(list.get(i) + "\t\t\t\t\t");
			}
			System.out.println();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null) {
				for (int i = 0; i < len; i++) {
					obj = (JSONObject) content.get(i);
					for (int j = 0; j < list.size(); j++) {
						System.out.print(obj.get(list.get(j)) + "\t\t\t");
					}
					System.out.println();

				}
			}

		}

	}

	private static void updatefromTable() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		String name;
		String pkey = "";
		Object value = null;
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag = false, loop = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
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
			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && flag == true) {
				for (int i = 0; i < len; i++) {

					obj = (JSONObject) content.get(i);
					if (obj.get(pkey).equals(prim)) {
						JSONObject newObj = new JSONObject();
						newObj.put(pkey, prim);
						BufferedReader brmeta = new BufferedReader(new FileReader(tname + "_meta.txt"));
						while ((name = brmeta.readLine()) != null) {
							String s[] = name.split(" ");
							if (!s[0].equals(pkey)) {
								System.out.println("Enter value for " + s[0]);
								//
								if (Integer.parseInt(s[1]) == 1 || Integer.parseInt(s[1]) == 2) {
									loop = true;
									while (loop) {
										try {
											value = sc.nextLong();
											loop = false;
										} catch (InputMismatchException e) {
											System.out.println("Invalid value!");
											sc.next();
										}
									}
								} else if (Integer.parseInt(s[1]) == 5) {
									loop = true;
									while (loop) {
										try {
											value = sc.nextBoolean();
											loop = false;
										} catch (InputMismatchException e) {
											System.out.println("Invalid value!");
											sc.next();
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
		String name;
		String pkey = "";
		JSONParser parser = new JSONParser();
		JSONObject obj;
		boolean flag = false;

		if (!tablename.contains(tname)) {
			System.out.println("Table doesn't Exist");
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

			JSONArray list = new JSONArray();
			JSONArray content = (JSONArray) parser.parse(new FileReader(tname + ".json"));
			int len = content.size();
			if (content != null && flag == true) {
				for (int i = 0; i < len; i++) {

					obj = (JSONObject) content.get(i);
					if (!obj.get(pkey).equals(prim)) {
						list.add(obj);
					}
				}
			}
			FileWriter file = new FileWriter(tname + ".json");
			file.write("");
			file.write(list.toJSONString());
			file.close();

		}

	}

	private static void inserttoTable() throws IOException, ParseException {

		System.out.println("Enter Table Name: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
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
			BufferedReader br = new BufferedReader(new FileReader(tname + "_meta.txt"));
			while ((name = br.readLine()) != null) {
				String s[] = name.split(" ");
				System.out.println("Enter value for " + s[0]);
				// type check start
				if (Integer.parseInt(s[1]) == 1 || Integer.parseInt(s[1]) == 2) {
					loop = true;
					while (loop) {
						try {
							value = sc.nextLong();
							loop = false;
						} catch (InputMismatchException e) {
							System.out.println("Invalid value!");
							sc.next();
						}
					}
				} else if (Integer.parseInt(s[1]) == 5) {
					loop = true;
					while (loop) {
						try {
							value = sc.nextBoolean();
							loop = false;
						} catch (InputMismatchException e) {
							System.out.println("Invalid value!");
							sc.next();
						}
					}
				} else {
					value = sc.nextLine();
				}
				// type check ends

				newObj.put(s[0], value);

			}
			br.close();
			
			if (content.size()!=0 && list.contains(newObj.get(pkey).toString())) {
				System.out.println("Primary key already exist. Record cannot be added.");
			} else {
				content.add(newObj);
			}
			FileWriter file = new FileWriter(tname + ".json");
			file.write("");
			file.write(content.toJSONString());
			file.close();
		}

	}

	private static void listTables() {
		for (String str : tablename) {
			System.out.println(str);
		}

	}

	private static void deleteTable() {
		System.out.println("Enter table name to be deleted: ");
		Scanner sc = new Scanner(System.in);
		String tname = sc.next();
		if (!tablename.contains(tname))
			System.out.println("Table does not exist");
		else {
			tablename.remove(tname);
			File file = new File(tname + "_meta.txt");
			if (file.exists())
				file.delete();
			file = new File(tname + ".json");
			if (file.exists())
				file.delete();
		}

	}

	private static void createTable() throws IOException {

		Scanner sc = new Scanner(System.in);
		String dt;
		System.out.println("Enter the Table Name");
		String tname = sc.next();
		ArrayList<String> types = new ArrayList<String>();
		types.add("1");
		types.add("2");
		types.add("3");
		types.add("4");
		types.add("5");
		ArrayList<String> colNames = new ArrayList<String>();
		int slen=0;
		if (tablename.contains(tname))
			System.out.println("Table already Exist");

		else {
			BufferedWriter br = new BufferedWriter(new FileWriter(tname + "_meta.txt"));
			System.out.println("Enter number of columns in the Table");
			int count = sc.nextInt();//todo
			sc.reset();
			System.out.println("Available Datatypes Integer(1) Float(2) String(3) Date(4) Boolean(5)");
			for (int i = 0; i < count; i++) {
				String line;
				sc.nextLine();
				do{
				System.out.println("Enter Column Name");
				line = sc.nextLine();
					String s[] = line.split(" ");
					slen=s.length;
				}while(slen>1);
				
				colNames.add(line);
				do {
					System.out.println("Enter Datatype Value e.g 1 for Integer : ");
					dt = sc.next();
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
			do {
				System.out.println("What is the primary key for this Table: ");
				primkey = sc.nextLine();
			} while (!colNames.contains(primkey));
			tablekey.put(tname, primkey);
			tablename.add(tname);
			System.out.println("New Table " + tname + " Created. ");

		}

	}
}
