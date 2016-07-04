package dbparser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.parser.ParseException;

public class parser {
	void parseSQL(String inSQL) throws IOException, ParseException {
		String delims = " ";
		String[] tokens = inSQL.split(delims);
		String inputStr = inSQL;// .toLowerCase();
		switch (tokens[0]) {
		case "select":
			parseSelect(inputStr);
			break;
		case "insert":
			parseInsert(inputStr);
			break;
		case "update":
			parseUpdate(inputStr);
			break;
		case "delete":
			parseDelete(inputStr);
			break;
		default:
			System.out.println("Invalid SQL statement!");
			break;
		}
	}

	private static void parseSelect(String inSQL) throws IOException, ParseException {

		String attributes = "";
		String tables = "";
		String whereCond = "";
		String orderBy = "";
		String tname = "";
		ArrayList<String> tableList = new ArrayList<String>();
		ArrayList<String> attributeList = new ArrayList<String>();
		ArrayList<String> condList = new ArrayList<String>();
		boolean whereFlag = false;
		boolean orderFlag = false;
		boolean ascending = true;
		boolean andFlag = false;
		boolean orFlag = false;
		boolean joinFlag = false;

		if (inSQL.toLowerCase().contains("select")) {
			String delimSel = "select\\s*|SELECT\\s*";
			String[] withoutSel = inSQL.split(delimSel);

			if (!withoutSel[1].equals(null) && withoutSel[1].toLowerCase().contains("from")) {
				String delimFrom = "\\s*from\\s*|\\s*FROM\\s*";
				String[] withoutFrom = withoutSel[1].split(delimFrom);
				attributes = withoutFrom[0];

				if (inSQL.toLowerCase().contains(" where ")) {
					whereFlag = true;
				}
				if (inSQL.toLowerCase().contains(" order by ")) {
					orderFlag = true;
				}

				if (whereFlag) {
					String delimWhere = "\\s*where\\s*|\\s*WHERE\\s*";
					String[] withoutWhere = withoutFrom[1].split(delimWhere);
					tables = withoutWhere[0];

					if (orderFlag) {
						String delimOrder = "\\s*order by\\s*|\\s*ORDER BY\\s*";
						String[] withoutOrder = withoutWhere[1].split(delimOrder);
						whereCond = withoutOrder[0];

						String delimSem = "\\s*;\\s*";
						String[] withoutSem = withoutOrder[1].split(delimSem);
						orderBy = withoutSem[0];
					} else {
						String delimSem = "\\s*;\\s*";
						String[] withoutSem = withoutWhere[1].split(delimSem);
						whereCond = withoutSem[0];
					}
				} else {
					if (orderFlag) {
						String delimOrder = "\\s*order by\\s*|\\s*ORDER BY\\s*";
						String[] withoutOrder = withoutFrom[1].split(delimOrder);
						tables = withoutOrder[0];

						String delimSem = "\\s*;\\s*";
						String[] withoutSem = withoutOrder[1].split(delimSem);
						orderBy = withoutSem[0];
					} else {
						String delimSem = "\\s*;\\s*";
						try{
						String[] withoutSem = withoutFrom[1].split(delimSem);
						tables = withoutSem[0];
						}catch(Exception e){
							System.out.println("Wrong SQL!");
							return;
						}
					}
				}

				if (inSQL.toLowerCase().contains("order by") && !whereFlag) {

				}
			} else {
				System.out.println("Wrong SQL!");
				return;
			}
		} else {
			System.out.println("Wrong SQL!");
			return;
		}

		// finding the attributes
		if (attributes.contains(",")) {
			String[] attributestemp = attributes.split(",");
			for (String str : attributestemp) {
				if (!str.isEmpty() && !str.equals(" ")) {
					attributeList.add(str.trim());
				}
			}
		} else {
			attributeList.add(attributes.trim());
		}

		// finding the tables
		if (tables.contains(",")) {
			String[] tablestemp = tables.split(",");
			for (String str : tablestemp) {
				if (!str.trim().isEmpty() && !str.trim().equals("")) {
					tableList.add(str.trim());
					joinFlag = true;
				}
			}
		} else {
			if(tables.trim().equals("")){
				System.out.println("No tables specified in the SQL!");
				return;
			}
			tableList.add(tables.trim());
		}

		// finding the conditions, if any
		if (whereFlag) {
			if (whereCond.toLowerCase().contains(" and ") || whereCond.toLowerCase().contains(" or ")) {
				String[] where = whereCond.split(" and | AND | or | OR ");
				for (String str : where) {
					if (!str.trim().isEmpty() && !str.trim().equals(" ")) {
						condList.add(str.trim());
					}
				}
				if (whereCond.toLowerCase().contains(" and ")) {
					andFlag = true;
				} else {
					orFlag = true;
				}
			} else {
				condList.add(whereCond.trim());
				andFlag = true;
			}
		}

		// finding order by order, if any
		if (orderFlag) {
			if (orderBy.toLowerCase().contains(" desc")) {
				ascending = false;
				orderBy = orderBy.replace("desc", "").trim();
			} else {
				if (orderBy.toLowerCase().contains("asc")) {
					orderBy = orderBy.replace("asc", "").trim();
				} else {
					orderBy = orderBy.trim();
				}
			}
		}

		entryClass entry = new entryClass();
		
		for(String str:tableList){
			if(!entry.tablename.contains(str)){
				System.out.println("Table "+str+" does not exist! Wrong SQL!");
				return;
			}
		}
		
		if(joinFlag){
			if(tableList.size()>2){
				System.out.println("More than 2 tables. Cannot join. Returning to main menu!");
				return;
			}else{
				String cond = "";
				boolean flag = false;
				for (String str : condList) {
					if (str.contains("=")) {
						String[] currCond = str.split("((?<==)|(?==))");
						String pkey1 = currCond[0].trim();
						String operator = currCond[1].trim();
						String pkey2 = currCond[2].trim();
						if (tableList.contains(pkey1.replace("_pkey", ""))
								&& tableList.contains(pkey2.replace("_pkey", "")) && operator.equals("=")) {
							flag = true;
							cond = str;
							break;
						}
					}
				}
				condList.remove(cond);
				if(!flag){
					System.out.println("Wrong/Absent join condition!");
					return;
				}
				entry.joinOnPkey(tableList.get(0), tableList.get(1));
				tname = tableList.get(0) + "_" + tableList.get(1);
			}
		}else{
			tname = tableList.get(0);
			//creating a temp file to perform operations on
			entry.createTempTable(tname, tname+"_temp");
		}
		
		if(whereFlag){
			//get pkey of the table
			//check if the condList contains that primary key
			//remove that condition from condList and call function for search based on pkey
			boolean pkeyFlag = false;
			String pkey = entry.getPkey(tname);
			String cond = "";
			for (String str : condList) {
				if (str.contains(pkey)) {
					pkeyFlag = true;
					cond = str;
					break;
				}
			}
			if (pkeyFlag) {
				condList.remove(cond);
				String[] currCond = cond.split("((?<=>|<|=)|(?=>|<|=))");
				String operator = currCond[1].trim();
				String valueToSearch = currCond[2].trim();
				try {
					Integer.parseInt(valueToSearch);
				} catch (Exception e) {
					System.out.println("Value to search on the primary key is not integer!");
					return;
				}
				entry.searchOnPrimKey(tname+"_temp", operator, Integer.parseInt(valueToSearch));
			}
			if (andFlag) {
				int ret = entry.searchForSQL(tname+"_temp", condList);
				if (ret == -1){
					return;
				}
			}else if(orFlag){
				int ret = entry.searchForOrSQL(tname+"_temp", condList);
				if (ret == -1){
					return;
				}
			}
		}
		

		//order by handling, if present
		if(orderFlag){
			entry.sortTableSQL(tname+"_temp", orderBy, ascending);
		}
		
		//taking projection/selection
		entry.projectionForSQL(tname+"_temp", attributeList);
		
		//printing the table
		entry.printTable(tname+"_temp");
		
		entry.deleteTempTable(tname + "_temp");
	}

	private static void parseInsert(String inSQL) throws IOException, ParseException {
		String attributes = "";
		String table = "";
		String values = "";
		ArrayList<String> attributeList = new ArrayList<String>();
		ArrayList<String> valueList = new ArrayList<String>();

		String delimIns = "insert into\\s*|INSERT INTO\\s*";
		String[] withoutIns = inSQL.split(delimIns);

		if (!withoutIns[1].equals(null) && withoutIns[1].toLowerCase().contains("values")) {
			String delimBrac = "\\(";
			String[] withoutBrac = withoutIns[1].split(delimBrac,2);
			table = withoutBrac[0].trim();

			String delimBrac2 = "\\)\\s+values\\s+|\\)\\s+VALUES\\s+";
			String[] withoutBrac2 = withoutBrac[1].split(delimBrac2);
			attributes = withoutBrac2[0].trim();

			String[] valWithoutBrac = withoutBrac2[1].split("\\(");
			String[] valWithoutBrac2 = valWithoutBrac[1].split("\\);");
			values = valWithoutBrac2[0].trim();

		} else {
			System.out.println("Wrong SQL!");
			return;
		}

		// finding the attributes
		if (attributes.contains(",")) {
			if (!attributes.contains("'")) {
				String[] attributestemp = attributes.split(",");
				for (String str : attributestemp) {
					if (!str.isEmpty() && !str.equals(" ")) {
						attributeList.add(str.trim());
					}
				}
			}else{
				//TODO write regex for parsing!? this is for the next step, not this one
			}
		} else {
			attributeList.add(attributes.trim());
		}
		
		// finding the values
		if (values.contains(",")) {
			String[] valuestemp = values.split(",");
			for (String str : valuestemp) {
				if (!str.isEmpty() && !str.equals(" ")) {
					valueList.add(str.trim());
				}
			}
		} else {
			valueList.add(values.trim());
		}
		
		//insert to table
		entryClass entry = new entryClass();
		entry.insertSQL(table, attributeList, valueList);
	}

	private static void parseUpdate(String inSQL) throws IOException, ParseException {
		// 1. parse and apply where conditions if any
		// 2. get the records that are to be updated in a new file with the updated values
		// 3. search the original file and replace the rows in the file generated
		// above in place of existing rows in the table
		
		String whereCond = "";
		String setVals = "";
		String table = "";
		boolean andFlag = false;
		boolean orFlag = false;
		boolean whereFlag = false;
		ArrayList<String> condList = new ArrayList<String>();
		ArrayList<String> setList = new ArrayList<String>();

		String delimUpd = "update\\s+|UPDATE\\s+";
		String[] withoutUpd = inSQL.split(delimUpd);

		if (!withoutUpd[1].equals(null) && withoutUpd[1].toLowerCase().contains(" set ")) {
			String delimSet = " set | SET ";
			String[] withoutSet = withoutUpd[1].split(delimSet);
			table = withoutSet[0].trim();

			if (!withoutSet[1].toLowerCase().contains(" where ")) {
				setVals = withoutSet[1].replace(";", "").trim();
			} else {
				whereFlag = true;
				String[] withoutWhere = withoutSet[1].split(" where | WHERE ");
				whereCond = withoutWhere[1].replace(";", "").trim();
				setVals = withoutWhere[0];
			}

			String[] setConds = setVals.split(",");
			for (String str : setConds) {
				if(str.contains(table+"_pkey")){
					System.out.println("Tried to update primary. Kindly never try this again.");
					return;
				}
				if (!str.trim().isEmpty() && !str.trim().equals("")) {
					setList.add(str.trim());
				}
			}

			if (whereFlag) {
				if (whereCond.toLowerCase().contains(" and ") || whereCond.toLowerCase().contains(" or ")) {
					String[] where = whereCond.split(" and | AND | or | OR ");
					for (String str : where) {
						if (!str.trim().isEmpty() && !str.trim().equals("")) {
							condList.add(str.trim());
						}
					}
					if (whereCond.toLowerCase().contains(" and ")) {
						andFlag = true;
					} else {
						orFlag = true;
					}
				} else {
					condList.add(whereCond.trim());
					andFlag = true;
				}
			}
		} else {
			System.out.println("Wrong SQL!");
			return;
		}
		
		// creating a temp file to perform operations on
		entryClass entry = new entryClass();
		entry.createTempTable(table, table + "_temp");
		
		//applying where conditions
		if(whereFlag){
			if(whereFlag){
				if(andFlag){
					int ret = entry.searchForSQL(table+"_temp", condList);
					if (ret == -1){
						return;
					}
				}else if(orFlag){
					int ret = entry.searchForOrSQL(table+"_temp", condList);
					if (ret == -1){
						return;
					}
				}
			}
		}
		
		//get primary keys of values to be updated
		ArrayList<String> pkeys = entry.getPkeys(table+"_temp");
		
		//update table
		entry.updateSQL(table,setList,pkeys);
		System.out.println("Updated successfully!");
		
		entry.deleteTempTable(table + "_temp");
	}

	private static void parseDelete(String inSQL) throws IOException, ParseException {
		// parse
		// collect primary key for all the records that are to be deleted
		// delete records
		String whereCond = "";
		String table = "";
		boolean andFlag = false;
		boolean orFlag = false;
		ArrayList<String> condList = new ArrayList<String>();

		String delimDel = "delete from\\s+|DELETE FROM\\s+";
		String[] withoutDel = inSQL.split(delimDel);

		if (!withoutDel[1].equals(null) && withoutDel[1].toLowerCase().contains(" where ")) {
			String delimWhere = " where | WHERE ";
			String[] withoutWhere = withoutDel[1].split(delimWhere);
			table = withoutWhere[0].trim();

			whereCond = withoutWhere[1].replace(";", "").trim();

			if (whereCond.toLowerCase().contains(" and ") || whereCond.toLowerCase().contains(" or ")) {
				String[] where = whereCond.split(" and | AND | or | OR ");
				for (String str : where) {
					if (!str.trim().isEmpty() && !str.trim().equals("")) {
						condList.add(str.trim());
					}
				}
				if (whereCond.toLowerCase().contains(" and ")) {
					andFlag = true;
				} else {
					orFlag = true;
				}
			} else {
				condList.add(whereCond.trim());
				andFlag = true;
			}

		} else {
			System.out.println("Wrong SQL!");
			return;
		}

		// creating a temp file to perform operations on
		entryClass entry = new entryClass();
		entry.createTempTable(table, table + "_temp");

		if (andFlag) {
			entry.deleteForSQL(table, condList);
		} else if (orFlag) {
			entry.deleteForOrSQL(table, condList);
		}

		entry.deleteTempTable(table + "_temp");
	}
}
