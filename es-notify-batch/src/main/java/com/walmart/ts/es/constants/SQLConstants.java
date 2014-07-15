package com.walmart.ts.es.constants;

public final class SQLConstants {

	public static final String CREATE_KEYSPACE = 
				"CREATE KEYSPACE loggingmonitor" +
				" WITH replication = {'class' : 'NetworkTopologyStrategy', 'EDC_Hadoop' : %s, 'EDC_Solr' : %s};";

	public static final String UPDATE_KEYSPACE = 
			"UPDATE KEYSPACE loggingmonitor" +
			" WITH replication = {'class' : 'NetworkTopologyStrategy', 'EDC_Hadoop' : %s, 'EDC_Solr' : %s};";

	
	public static final String CREATE_ALERT_CF_INDEX = 
			"CREATE TABLE alert_index(" +			 
			" key text," +				
			" createDate timestamp," +
			" fkey text," +		
			" PRIMARY KEY(key, createDate));";
	
	public static final String CREATE_NOTIFICATION_CF = 
			"CREATE TABLE notification(" +			 
			" key text PRIMARY KEY," +				
			" createDate timestamp);";

	public static final String INSERT_NOTIFICATION = 
			"INSERT INTO notification(key, createDate) VALUES(?, ?) USING TTL ?;";
	
	public static final String GET_NOTIFICATION = 
			"SELECT createDate FROM notification where key = ?;";
	
	public static final String INSERT_ALERT_INDEX = 
			"INSERT INTO alert_index(key, createDate, fkey) VALUES(?, ?, ?);";
	public static final String CREATE_ALERT_CF = 
				"CREATE TABLE alert("
				+ " key text PRIMARY KEY, " 
				+ " teamId text, "
				+ " userId text, "
				+ " cron text, "
				+ " createDate timestamp, "
				+ " blackoutDurationType text, "
				+ " blackoutDuration int, "
				+ " source text, "
				+ " fieldKey text, "
				+ " fieldValue text, "
				+ " maxOccurance int, "
				+ " alertDurationType text, "
				+ " alertDuration int);";
		
	public static final String GET_KEYS_QUERY= 
		      "SELECT " 
			+ " fkey, "
			+ " FROM alert_index"
			+ " WHERE key = ? "			
			+ " AND createDate > ? "
			+ " AND createDate < ? "
			+ " ORDER BY createDate DESC;";			
		
	public static final String GET_ALERT_BY_SOURCE_QUERY= 
		      "SELECT " 
			+ " teamId,userId,cron,blackoutDurationType,blackoutDuration,source,fieldKey,fieldValue,maxOccurance,alertDurationType,alertDuration "
			+ " FROM alert"
			+ " WHERE source = ?;";
		
	public static final String ALERT_CF_NAME = "alert";
	public static final String P_KEY = "PRIMARY KEY";
	public static final String QUERY_DELIMITER = ", ";
	public static final String PREPARED_STATEMENT_FLAG = "?";
	public static final String FOREIGN_KEY="fkey";

}
