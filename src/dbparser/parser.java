package dbparser;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.parser.ParseException;

import jdbm.btree.BTree;

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
		long recid;
		BTree tree;

//		try {
			if (inSQL.toLowerCase().contains("select")) {
				String delimSel = "select\\s*|SELECT\\s*";
				String[] withoutSel = inSQL.split(delimSel);
				if (withoutSel.length < 2) {
					System.out.println("Wrong SQL!");
					return;
				}
				if (!withoutSel[1].equals(null) && withoutSel[1].toLowerCase().contains("from")) {
					String delimFrom = "\\s*from\\s*|\\s*FROM\\s*";
					String[] withoutFrom = withoutSel[1].split(delimFrom);
					if (withoutFrom.length < 2) {
						System.out.println("Wrong SQL!");
						return;
					}
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
						if (withoutWhere.length < 2) {
							System.out.println("Wrong SQL!");
							return;
						}
						tables = withoutWhere[0];

						if (orderFlag) {
							String delimOrder = "\\s*order by\\s*|\\s*ORDER BY\\s*";
							String[] withoutOrder = withoutWhere[1].split(delimOrder);
							if (withoutOrder.length < 2) {
								System.out.println("Wrong SQL!");
								return;
							}
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
							if (withoutOrder.length < 2) {
								System.out.println("Wrong SQL!");
								return;
							}
							tables = withoutOrder[0];

							String delimSem = "\\s*;\\s*";
							String[] withoutSem = withoutOrder[1].split(delimSem);
							orderBy = withoutSem[0];
						} else {
							String delimSem = "\\s*;\\s*";
							try {
								String[] withoutSem = withoutFrom[1].split(delimSem);
								tables = withoutSem[0];
							} catch (Exception e) {
								System.out.println("Wrong SQL!");
								return;
							}
						}
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
				if (attributestemp.length < 2) {
					System.out.println("Wrong SQL!");
					return;
				}
				for (String str : attributestemp) {
					if (!str.isEmpty() && !str.trim().equals("")) {
						attributeList.add(str.trim());
					}
				}
				if (attributeList.size() < 2) {
					System.out.println("Wrong SQL!");
					return;
				}
			} else {
				attributeList.add(attributes.trim());
			}
			if (attributeList.isEmpty()) {
				System.out.println("Wrong SQL! Wrong attributes!");
				return;
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
				if (tableList.size() < 2) {
					System.out.println("Wrong SQL!");
					return;
				}
			} else {
				if (tables.trim().equals("")) {
					System.out.println("No tables specified in the SQL!");
					return;
				}
				if (tables.trim().contains(" ")) {
					System.out.println("Tables not comma sepatared!");
					return;
				}
				tableList.add(tables.trim());
			}

			// finding the conditions, if any
			if (whereFlag) {
				if (whereCond.toLowerCase().contains(" and ") || whereCond.toLowerCase().contains(" or ")) {
					String[] where = whereCond.split(" and | AND | or | OR ");
					if (where.length < 2) {
						System.out.println("Something wrong in the where clause!");
						return;
					}
					for (String str : where) {
						if (!str.trim().isEmpty() && !str.trim().equals(" ")) {
							condList.add(str.trim());
						}
					}
					if (condList.size() < 2) {
						System.out.println("Something wrong in the where clause!");
						return;
					}
					if (whereCond.toLowerCase().contains(" and ")) {
						andFlag = true;
					} else {
						orFlag = true;
					}
				} else {
					if (whereCond.trim().equals("")) {
						System.out.println("Where condition absent!");
						return;
					}
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
				if (orderBy.equals("")) {
					System.out.println("Order by attribute not supplied!");
					return;
				}
			}

			entryClass entry = new entryClass();

			for (String str : tableList) {
				if (!entry.tablename.contains(str)) {
					if (str.contains(" ")) {
						System.out.println("Wrong SQL!");
						return;
					}
					System.out.println("Table " + str + " does not exist! Wrong SQL!");
					return;
				}
			}

			if (joinFlag) {
				if (tableList.size() > 2) {
					System.out.println("More than 2 tables. Cannot join. Returning to main menu!");
					return;
				} else {
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
					if (!flag) {
						System.out.println("Wrong/Absent join condition!");
						return;
					}
					entry.joinOnPkey(tableList.get(0), tableList.get(1));
					tname = tableList.get(0) + "_" + tableList.get(1);
				}
			} else {
				tname = tableList.get(0);
				// creating a temp file to perform operations on
				entry.createTempTable(tname, tname + "_temp");
			}

			if (whereFlag) {
				// get pkey of the table
				// check if the condList contains that primary key
				// remove that condition from condList and call function for
				// search based on pkey
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
					String[] currCond = cond.split("((?<=>|<|=)|(?=>|<|=))");
					String operator = currCond[1].trim();
					String valueToSearch = currCond[2].trim();
					try {
						Integer.parseInt(valueToSearch);
					} catch (Exception e) {
						System.out.println("Value to search on the primary key is not integer!");
						return;
					}
					entry.searchOnPrimKey(tname, operator, Integer.parseInt(valueToSearch));
					condList.remove(cond);
				}
				if (andFlag) {
					if (entry.searchUsingIndex) {
						ArrayList<String> condToRem = new ArrayList<String>();
						for (String str : condList) {
							String delims = "((?<=>|<|=)|(?=>|<|=))";
							String[] currCond = str.split(delims);
							if (currCond.length > 3) {
								System.out.println("Wrong condition in where clause!");
								entry.searchUsingIndex = false;
								return;
							}
							String columnName = currCond[0].trim();
							String operator = currCond[1].trim();
							String valueToSearch = currCond[2].trim();
							if (valueToSearch.contains("'")) {
								valueToSearch = valueToSearch.replace("'", "").trim();
							}
							recid = entry.recman.getNamedObject(tname + "_" + columnName + "_btree");
							if (recid != 0) {
								HashMap<String, Integer> dtype = entry.returnDataType(tname);
								int ret = 0;
								if (dtype.get(columnName) == 1) {
									ret = entry.searchOnIndexedKey(tname, columnName, operator,
											Integer.parseInt(valueToSearch));
									if (ret != -1) {
										condToRem.add(str);
									}
								} else if (dtype.get(columnName) != 2) {
									ret = entry.searchOnIndexedKey2(tname, columnName, operator, valueToSearch);
									if (ret != -1) {
										condToRem.add(str);
									}
								}
								if (ret == -1) {
									System.out.println("Problem in searching by indexed attributes!");
									entry.searchUsingIndex = false;
									return;
								}

							}
						}
						for (String str : condToRem) {
							condList.remove(str);
						}
						entry.searchUsingIndex = false;
					}
					if (!condList.isEmpty()) {
						int ret = entry.searchForSQL(tname + "_temp", condList);
						if (ret == -1) {
							return;
						}
					}
				} else if (orFlag) {
					if (entry.searchUsingIndex) {
						ArrayList<String> condToRem = new ArrayList<String>();
						for (String str : condList) {
							String delims = "((?<=>|<|=)|(?=>|<|=))";
							String[] currCond = str.split(delims);
							if (currCond.length > 3) {
								System.out.println("Wrong condition in where clause!");
								entry.searchUsingIndex = false;
								return;
							}
							String columnName = currCond[0].trim();
							String operator = currCond[1].trim();
							String valueToSearch = currCond[2].trim();
							if (valueToSearch.contains("'")) {
								valueToSearch = valueToSearch.replace("'", "").trim();
							}
							recid = entry.recman.getNamedObject(tname + "_" + columnName + "_btree");
							if (recid != 0) {
								HashMap<String, Integer> dtype = entry.returnDataType(tname);
								int ret = 0;
								if (dtype.get(columnName) == 1) {
									ret = entry.searchOnIndexedKey3(tname, columnName, operator,
											Integer.parseInt(valueToSearch));
									if (ret != -1) {
										condToRem.add(str);
									}
								} else if (dtype.get(columnName) != 2) {
									ret = entry.searchOnIndexedKey4(tname, columnName, operator, valueToSearch);
									if (ret != -1) {
										condToRem.add(str);
									}
								}
								if (ret == -1) {
									System.out.println("Problem in searching by indexed attributes!");
									entry.searchUsingIndex = false;
									return;
								}

							}
						}
						for (String str : condToRem) {
							condList.remove(str);
						}
						entry.searchUsingIndex = false;
					}
					if (!condList.isEmpty()) {
						int ret = entry.searchForOrSQL(tname + "_temp", condList);
						if (ret == -1) {
							return;
						}
					}
				}
			}

			// order by handling, if present
			if (orderFlag) {
				entry.sortTableSQL(tname + "_temp", orderBy, ascending);
			}

			// taking projection/selection
			entry.projectionForSQL(tname + "_temp", attributeList);

			// printing the table
			entry.printTable(tname + "_temp");

			entry.deleteTempTable(tname + "_temp");
//		} catch (Exception e) {
//			System.out.println("Wrong select SQL!");
//		}
	}

	private static void parseInsert(String inSQL) throws IOException, ParseException {
		String attributes = "";
		String table = "";
		String values = "";
		ArrayList<String> attributeList = new ArrayList<String>();
		ArrayList<String> valueList = new ArrayList<String>();

		String delimIns = "insert into\\s*|INSERT INTO\\s*";
		String[] withoutIns = inSQL.split(delimIns);
		if(withoutIns.length<2){
			System.out.println("Wrong insert SQL!");
			return;
		}

		if (!withoutIns[1].trim().equals("") && withoutIns[1].toLowerCase().contains("values")) {
			try {
				String delimBrac = "\\(";
				String[] withoutBrac = withoutIns[1].split(delimBrac, 2);
				table = withoutBrac[0].trim();

				String delimBrac2 = "\\)\\s+values\\s+|\\)\\s+VALUES\\s+";
				String[] withoutBrac2 = withoutBrac[1].split(delimBrac2);
				attributes = withoutBrac2[0].trim();

				String[] valWithoutBrac = withoutBrac2[1].split("\\(");
				String[] valWithoutBrac2 = valWithoutBrac[1].split("\\);");
				values = valWithoutBrac2[0].trim();
			} catch (Exception e) {
				System.out.println("Wrong insert SQL!");
				return;
			}
		} else {
			System.out.println("Wrong insert SQL!");
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
		
		// finding the values
		if (values.contains(",")) {
			if (!values.contains("'")) {
				String[] valuestemp = values.split(",");
				for (String str : valuestemp) {
					if (!str.isEmpty() && !str.trim().equals("")) {
						valueList.add(str.trim());
					}
				}
			} else {
				values=values.replace("'", "");
				String[] valuestemp = values.split(",");
				for (String str : valuestemp) {
					if (!str.isEmpty() && !str.trim().equals("")) {
						valueList.add(str.trim());
					}
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
		if(withoutUpd.length<2){
			System.out.println("Wrong update SQL!");
			return;
		}

		if (!withoutUpd[1].equals(null) && withoutUpd[1].toLowerCase().contains(" set ")) {
			try {
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
					if (str.contains(table + "_pkey")) {
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
			} catch (Exception e) {
				System.out.println("Wrong update SQL!");
				return;
			}
		} else {
			System.out.println("Wrong update SQL!");
			return;
		}
		
		// creating a temp file to perform operations on
		entryClass entry = new entryClass();
//		entry.createTempTable(table, table + "_temp");
		
		//applying where conditions
		if (whereFlag) {
			if (andFlag) {
				int ret = entry.updateForSQL(table, setList, condList);
				if (ret == -1) {
					return;
				}
			} else if (orFlag) {
				int ret = entry.updateForOrSQL(table, setList, condList);
				if (ret == -1) {
					return;
				}
			}
		} else {
			int ret = entry.updateForSQLWithoutCond(table, setList);
			if (ret == -1) {
				return;
			}
		}

		//get primary keys of values to be updated
//		ArrayList<String> pkeys = entry.getPkeys(table+"_temp");
		
		//update table
//		entry.updateSQL(table,setList,pkeys);
		entry.recreateIndexOnCol(table);
		System.out.println("Updated successfully!");
		
//		entry.deleteTempTable(table + "_temp");
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
		if(withoutDel.length<2){
			System.out.println("Wrong delete SQL!");
			return;
		}

		if (!withoutDel[1].equals(null) && withoutDel[1].toLowerCase().contains(" where ")) {
			try {
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
			} catch (Exception e) {
				System.out.println("Wrong delete SQL!");
				return;
			}
		} else {
			System.out.println("Wrong delete SQL!");
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
