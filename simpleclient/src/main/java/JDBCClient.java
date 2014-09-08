/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.teiid.jdbc.TeiidDataSource;
import org.teiid.jdbc.TeiidStatement;

@SuppressWarnings("nls")
public class JDBCClient {
	
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	
	public static final String USERNAME_DEFAULT = "user";
	public static final String PASSWORD_DEFAULT = "user";
	
	public static void main(final String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("usage: JDBCClient <host> <port> <vdb> <sql-command>");
			System.exit(-1);
		}
		
		System.out.println("Executing using the TeiidDataSource");
		// this is showing how to make a Data Source connection. 
		
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		
		Callable c = new Callable(){
				public Connection call() throws Exception {
					System.out.println("In call");
					TeiidDataSource ds = new TeiidDataSource();
					ds.setDatabaseName(args[2]);
					ds.setUser(getUserName());
					ds.setPassword(getPassword());
					ds.setServerName(args[0]);
					ds.setPortNumber(Integer.valueOf(args[1]));
		
					ds.setShowPlan("on"); //turn show plan on
		                        System.out.println("pre ps.getConnection");  
					return ds.getConnection();
					
				}
		};
		
		Future future = executorService.submit(c);
		
		Connection cn = null;
		
                try {
                  cn =(Connection) future.get(500, TimeUnit.MILLISECONDS);
                } catch (Exception e){
                	e.printStackTrace();
                	future.cancel(true); //this method will stop the running underlying task
                }
		
                if (cn != null){
                	System.out.println("future not null");
                	execute(cn, args[3]);
		}
		
	}

	static String getUserName() {
		return (System.getProperties().getProperty(USERNAME) != null ? System.getProperties().getProperty(USERNAME) : USERNAME_DEFAULT );
	}
	
	static String getPassword() {
		return (System.getProperties().getProperty(PASSWORD) != null ? System.getProperties().getProperty(PASSWORD) : PASSWORD_DEFAULT );
	}
	
	public static boolean execute(Connection connection, String sql) throws Exception {
		try {
			Statement statement = connection.createStatement();
			
			boolean hasRs = statement.execute(sql);
			
			if (!hasRs) {
				int cnt = statement.getUpdateCount();
				System.out.println("----------------\r");
				System.out.println("Updated #rows: " + cnt);
				System.out.println("----------------\r");
			} else {
				ResultSet results = statement.getResultSet();
				ResultSetMetaData metadata = results.getMetaData();
				int columns = metadata.getColumnCount();
				System.out.println("Results");
				for (int row = 1; results.next(); row++) {
					System.out.print(row + ": ");
					for (int i = 0; i < columns; i++) {
						if (i > 0) {
							System.out.print(",");
						}
						System.out.print(results.getString(i+1));
					}
					System.out.println();
				}
				results.close();
			}
			System.out.println("Query Plan");
			System.out.println(statement.unwrap(TeiidStatement.class).getPlanDescription());
			
			statement.close();
			return hasRs;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (connection != null) {
				connection.close();
			}
		}		
	}

}
