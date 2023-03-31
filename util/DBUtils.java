package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import entity.DataTypeEnum;
import entity.FD;
import entity.Key;
import entity.Parameter;

/**
 * some tools here on database operations
 *
 */
public class DBUtils {
	public static Connection connectDB() {
		String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
	    String DB_URL = "jdbc:mysql://localhost:3306/freeman?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
	    		+ "&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=true";
	    String USER = "root";
	    String PASS = "zzxzzx";
	    Connection conn = null;
        try{
            Class.forName(JDBC_DRIVER);
        
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }
        return conn;

	}
	
	/**
	 * 
	 * @param tableName
	 * @param R
	 * @throws SQLException
	 */
	public static void createTable(String tableName,List<String> R) throws SQLException {
		Connection conn =connectDB();
		String sql = "CREATE TABLE `"+tableName+"` (  \n";
		sql += "`id` int NOT NULL AUTO_INCREMENT,\n";
		for(int i = 0;i< R.size();i ++) {
			String columnName = R.get(i);
			sql += "`"+columnName +"` varchar(50),\n";
		}
		sql += "PRIMARY KEY (`id`)\n ) CHARSET=utf8mb3";
		System.out.println("\n======================");
		System.out.println("creating table with name : "+tableName+" | schema : "+R.toString());
		System.out.println(sql);
		System.out.println("======================\n");
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
		stmt.close();
		conn.close();
	}
	
	public static void dropTable(String tableName) throws SQLException {
		Connection conn =connectDB();
		String sql = "DROP TABLE IF EXISTS `"+tableName+"`";
		System.out.println("\n======================");
		System.out.println("dropping table with name : "+tableName);
		System.out.println(sql);
		System.out.println("======================\n");
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
		stmt.close();
		conn.close();
	}
	
	/**
	 * insert data into databases
	 * @param tableName
	 * @param dataset
	 * @return cost time
	 * @throws SQLException
	 */
	public static double insertData(String tableName,List<List<String>> dataset) throws SQLException {
		if(dataset == null)
			return -1;
		if(dataset.isEmpty())
			return -1;
		Connection conn = connectDB();
		conn.setAutoCommit(false);//manual commit
		String insertSql = "INSERT INTO `"+tableName + "` VALUES ( NULL,";//increment column
		for(int i = 0;i < dataset.get(0).size();i ++) {
			if(i != (dataset.get(0).size() - 1))
				insertSql += " ? ,";
			else
				insertSql += " ? )";
		}
		PreparedStatement prepStmt1 = conn.prepareStatement(insertSql);
		System.out.println("\n==================");
		System.out.println("insert "+dataset.size()+" records into table : "+tableName);
		System.out.println(insertSql);
		
		long start = System.currentTimeMillis();
		int count = 0;
		for(List<String> data : dataset) {
			count ++;
			for(int i = 1;i <= data.size();i ++) {
				prepStmt1.setString(i, data.get(i-1));
			}
			prepStmt1.addBatch();//batch process
			if(count % 10000 == 0) {
				prepStmt1.executeBatch();
				conn.commit();
				prepStmt1.clearBatch();
			}
		}
		prepStmt1.executeBatch();
		conn.commit();//commit
		long end = System.currentTimeMillis();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		prepStmt1.close();
		conn.close();
		return (double)(end - start);
	}
	
	/**
	 * query 1
	 * SELECT R.A1,...,R.An,count(*) FROM R GROUP BY R.A1,...,R.An HAVING COUNT(*) > 1
	 * @param key
	 * @param tableName
	 * @return query time
	 * @throws SQLException
	 */
	public static double queryOne(Key key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(String attr : key.getAttributes()) {
			sql += "`"+tableName+"`.`"+attr+"`, ";
		}
		sql += "COUNT(*) FROM `"+tableName+"` GROUP BY ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "HAVING COUNT(*) > 1";
		
		System.out.println("\n======================");
		System.out.println("executing query 1...");
		System.out.println(sql);
		
		long start = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = System.currentTimeMillis();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("======================\n");
		return (double)(end - start);
	}
	
	/**
	 * query 1 for null uncertainty semantics
	 * SELECT R_1.A1,...,R_1.An,count(*) FROM (SELECT * FROM R WHERE A1 IS NOT NULL, ... , An IS NOT NULL) AS R_1 
	 * GROUP BY R_1.A1,...,R_1.An HAVING COUNT(*) > 1
	 * @param key
	 * @param tableName
	 * @return query time
	 * @throws SQLException
	 */
	public static double queryOneForNullUC(Key key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String subtable = " ( SELECT * FROM `"+tableName+"` WHERE ";
		for(int i = 0;i < key.getAttributes().size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != key.getAttributes().size() - 1)
				subtable += "`"+tableName+"`.`"+attr+"` IS NOT NULL AND ";
			else
				subtable += "`"+tableName+"`.`"+attr+"` IS NOT NULL ) AS `"+tableName+"_1` ";
		}
		String sql = "SELECT ";
		for(String attr : key.getAttributes()) {
			sql += "`"+tableName+"_1`.`"+attr+"`, ";
		}
		sql += "COUNT(*) FROM "+subtable+" GROUP BY ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"_1`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"_1`.`"+attr+"` ";
		}
		sql += "HAVING COUNT(*) > 1";
		
		System.out.println("\n======================");
		System.out.println("executing query 1 for null uncertainty semantics...");
		System.out.println(sql);
		
		long start = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = System.currentTimeMillis();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("======================\n");
		return (double)(end - start);
	}
	/**
	 * query 2
	 * SELECT R.A1,...,R.An FROM R, R AS R1 WHERE R.A1 = R1.A1 AND ... AND R.An = R1.An AND R.id <> R1.id
	 * @param key
	 * @param tableName
	 * @return query time
	 * @throws SQLException
	 */
	public static double queryTwo(Key key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "FROM `"+tableName+"` , `"+tableName+"` AS `"+tableName+"_1` WHERE ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.getAttributes().get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"` AND ";
			else
				sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"`";
		}
		sql += " AND `"+tableName+"`.`id` <> `"+tableName+"_1`.`id`";
		
		System.out.println("\n======================");
		System.out.println("executing query 2...");
		System.out.println(sql);
		
		long start = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = System.currentTimeMillis();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("======================\n");
		return (double)(end - start);
	}
	
	/**
	 * delete all data from table with specific condition
	 * @param tableName
	 * @param dataset
	 * @param whereCondition according condition to delete tuples
	 * @throws SQLException 
	 */
	public static void deleteData(String tableName,String whereCondition) throws SQLException {
		Connection conn = connectDB();
		System.out.println("\n==================");
		System.out.println("delete inserted records from table : "+tableName);
		String delSql = "DELETE FROM `"+tableName + "` WHERE "+whereCondition;//delete records

		System.out.println(delSql);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(delSql);
		
		System.out.println("==================\n");
		stmt.close();
		conn.close();
	}
	
	/**
	 * specify a set of attributes (we call key) as an unique constraint on a table
	 * @param key
	 * @param tableName
	 * @throws SQLException
	 */
	public static void addUnique(Key key,String tableName,String uniqueID) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		
		String unique_constraint ="ALTER TABLE `"+tableName+"` ADD UNIQUE `"+uniqueID+"` ( ";//create an unique constraint
		for(int i =0;i < key.size();i ++) {
			if(i != (key.size() -1))
				unique_constraint += "`"+key.getAttributes().get(i) + "`,";
			else
				unique_constraint += "`"+key.getAttributes().get(i) + "` )";
		}
		
		System.out.println("\n==================");
		System.out.println("create an unique constarint : ");
		System.out.println(unique_constraint);
		
		stmt.executeUpdate(unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	/**
	 * remove an unique constraint on a table
	 * @param tableName
	 * @param unique_id
	 * @throws SQLException
	 */
	public static void removeUnique(String tableName,String uniqueID) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String del_unique_constraint = "DROP INDEX `"+uniqueID+"` ON `"+tableName+"`";//delete an unique constraint
		
//		System.out.println("\n==================");
//		System.out.println("delete an unique constarint : ");
//		System.out.println(del_unique_constraint);
		
		stmt.executeUpdate(del_unique_constraint);
		
//		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
//	public static void addTrigger(List<FD> fd_list,String tableName,String trigger_id) throws SQLException {
//		String delete_sql = "DELETE FROM `"+tableName+"` WHERE `"+tableName+"`.`id` = new.`id`";
//		String select_sql = "SELECT * FROM `"+tableName+"` WHERE";
//		for(int i = 0;i < fd_list.size();i ++) {
//			FD fd = fd_list.get(i);
//			if(fd.getLeftHand().isEmpty())
//				continue;
//			select_sql += " ( ";
//			for(String attr : fd.getLeftHand()) {
//				select_sql += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` AND ";
//			}
//			select_sql += " ( ";
//			for(int j = 0;j < fd.getRightHand().size();j ++) {
//				if(j != fd.getRightHand().size() - 1)
//					select_sql += "`"+tableName+"`.`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"` OR ";
//				else
//					select_sql += "`"+tableName+"`.`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"` ) ) ";
//			}
//			
//			
//			if(i != fd_list.size() - 1) {
//				select_sql += " OR ";
//			}
//			
//		}
//		
//		String TRIGGER = "CREATE TRIGGER `"+trigger_id+"`\r\n"
//				+ "AFTER INSERT ON `"+tableName+"`\r\n"
//				+ "FOR EACH ROW\r\n"
//				+ "BEGIN \r\n"
//				+ "   set @violation = IF(EXISTS("+select_sql+"),'YES','NO');\r\n"
//				+ "   if @violation = 'YES' THEN\r\n"
//				+ "       "+delete_sql+" ;\r\n"
//				+ "   end if;\r\n"
//				+ "END ";
//		
//		
//		Connection conn = connectDB();
//		Statement stmt = conn.createStatement();
//		System.out.println("add TRIGGER "+trigger_id+" into table "+tableName+"...");
//		System.out.println(TRIGGER);
//		stmt.executeUpdate(TRIGGER);
//		
//		stmt.close();
//		conn.close();
//	}
	
	/**
	 * Given an FD [LICENSE#] -> [CAR-SERIAL#, OWNER], using following to validate FD in trigger.
	 * SELECT * FROM ( SELECT * FROM `Traffic` WHERE `Traffic`.`LICENSE#` = new.`LICENSE#` ) as t1 
	 * WHERE t1.`CAR-SERIAL#` != new.`CAR-SERIAL#` OR t1.`OWNER` != new.`OWNER` )
	 * @param fd_list
	 * @param tableName
	 * @param trigger_id
	 * @throws SQLException
	 */
	public static void addTrigger(List<FD> fd_list,String tableName,String trigger_id) throws SQLException {
		String delete_sql = "DELETE FROM `"+tableName+"` WHERE `"+tableName+"`.`id` = new.`id`";
		List<String> FD_check_query_list = new ArrayList<>();//each string to validate one FD
		
		for(int i = 0;i < fd_list.size();i ++) {
			String FDLeftFilter = " SELECT * FROM `"+tableName+"` WHERE ";
			String FDRightFilter = "";
			
			FD fd = fd_list.get(i);
			if(fd.getLeftHand().isEmpty())
				continue;
			for(int m = 0;m < fd.getLeftHand().size();m ++) {
				String attr = fd.getLeftHand().get(m);
				if(m != fd.getLeftHand().size() - 1)
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` AND ";
				else
					FDLeftFilter += "`"+tableName+"`.`"+attr+"` = new.`"+attr+"` ";
			}
			
			for(int j = 0;j < fd.getRightHand().size();j ++) {
				if(j != fd.getRightHand().size() - 1)
					FDRightFilter += "t1.`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"` OR ";
				else
					FDRightFilter += "t1.`"+fd.getRightHand().get(j)+"` != new.`"+fd.getRightHand().get(j)+"`  ";
			}
			
			String valid_sql = "SELECT * FROM ("+FDLeftFilter+") as t1 WHERE "+FDRightFilter;
			FD_check_query_list.add(valid_sql);
		}
		
		String union = "";
		for(int i = 0;i < FD_check_query_list.size();i ++) {
			if(i != FD_check_query_list.size() - 1)
				union += "("+FD_check_query_list.get(i)+") UNION ALL ";
			else
				union += "("+FD_check_query_list.get(i)+") ";
		}
		
		String TRIGGER = "CREATE TRIGGER `"+trigger_id+"`\r\n"
				+ "AFTER INSERT ON `"+tableName+"`\r\n"
				+ "FOR EACH ROW\r\n"
				+ "BEGIN \r\n"
				+ "   set @violation = IF(EXISTS("+union+"),'YES','NO');\r\n"
				+ "   if @violation = 'YES' THEN\r\n"
				+ "       "+delete_sql+" ;\r\n"
				+ "   end if;\r\n"
				+ "END ";
		
		
		Connection conn = connectDB();
		Statement stmt = conn.createStatement();
		System.out.println("add TRIGGER "+trigger_id+" into table "+tableName+"...");
		System.out.println(TRIGGER);
		stmt.executeUpdate(TRIGGER);
		
		stmt.close();
		conn.close();
	}
	
	public static void removeTrigger(String tableName,String trigger_id) throws SQLException {
		String remove_sql = "DROP TRIGGER IF EXISTS `"+trigger_id+"`";
		Connection conn = connectDB();
		Statement stmt = conn.createStatement();
//		System.out.println("remove TRIGGER "+trigger_id+" from table "+tableName+"...");
//		System.out.println(remove_sql);
		stmt.executeUpdate(remove_sql);
		
		stmt.close();
		conn.close();
	}
	
	
	
	/**
	 * 
	 * @param para
	 * @return database table name for corresponding data set name
	 */
	public static String getDBTableName(Parameter para) {
		String tableName = null;
		switch(para.dataset.DataType) {
			case COMPLETE:
				tableName = para.dataset.name;
				break;
			case NULL_EQUALITY:
				tableName = para.dataset.name+"(nulleq)";
				break;
			case NULL_UNCERTAINTY:
				tableName = para.dataset.name+"(nulluc)";
				break;
		}
		return tableName;
	}
	/**
	 * set null marker in databases as Null value
	 * @param para
	 * @throws SQLException 
	 */
	public static void setNullMarkerAsNull(Parameter para) {
		Connection conn = null;
		Statement stmt = null;
		String tableName = getDBTableName(para);
		try {
			conn = connectDB();
			stmt = conn.createStatement();
			for(int attr = 0; attr < para.dataset.col_num; attr ++) {
				String update = "Update `"+tableName+"` set `"+attr+"` = NULL where `"+attr+"` = '"+para.dataset.nullMarker+"'";
				stmt.executeUpdate(update);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		Utils.getParameterList(Arrays.asList("pdbx"), DataTypeEnum.NULL_UNCERTAINTY).forEach(DBUtils::setNullMarkerAsNull);
	}

}
