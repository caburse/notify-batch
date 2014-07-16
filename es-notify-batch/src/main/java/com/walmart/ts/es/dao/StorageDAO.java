package com.walmart.ts.es.dao;

import java.lang.reflect.Field;

import org.apache.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.constants.PropertiesConstants;
import com.walmart.ts.es.constants.SQLConstants;
import com.walmart.ts.es.model.CassandraTypes;
import com.walmart.ts.es.util.PropertyUtil;

public class StorageDAO {


	private static final Logger LOGGER = Logger.getLogger(StorageDAO.class);
	private static Session session;
	private static Cluster cluster;
	private static ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;	
	
	public StorageDAO() {		
		init(PropertyUtil.getInstance().getString(PropertiesConstants.SERVERS), 
				PropertyUtil.getInstance().getString(PropertiesConstants.KEYSPACE));
	}
	
	public StorageDAO(String servers, String keyspace) {		
		init(servers, keyspace);
	}
		
	/**
	 * initialize cluster and session
	 * @param servers
	 * @param keyspace
	 */
	protected void init(String servers, String keyspace) {
//		String username = PropertyUtil.getInstance().getString(PropertiesConstants.USER_NAME);
//		String password = CryptoUtil.decrypt(PropertyUtil.getInstance().getString(PropertiesConstants.TOKEN));
		
//		cluster = Cluster.builder().addContactPoints(servers.split(",")).withCredentials(username, password).build();
		cluster = Cluster.builder().addContactPoints(servers.split(",")).build();
		Metadata metadata = cluster.getMetadata();
		
		LOGGER.info(String.format("Connected to cluster: %s \n",	metadata.getClusterName()));

		for (Host host : metadata.getAllHosts()) {
			LOGGER.info(String.format("Datatacenter: %s; Host: %s; Rack: %s\n",
			host.getDatacenter(), host.getAddress(), host.getRack()));
		}
		
		if (session == null) {
			try{
				session = (keyspace == null || keyspace.isEmpty())?cluster.connect():cluster.connect(keyspace);
			}catch(Exception e){
				LOGGER.warn(e);				
				session = cluster.connect();
			}
		}
	}

	/**
	 * Execute custom query
	 * @param cql
	 * @return
	 */
	public ResultSet execute(String cql) {
		LOGGER.info(cql);
		Statement cqlQuery = new SimpleStatement(cql)
			.setConsistencyLevel(consistencyLevel)
			.enableTracing();
		return session.execute(cqlQuery);
	}
	
	/**
	 * Execute custom query with prepared statement
	 * @param cql
	 * @return
	 */
	public ResultSet execute(String cql, Object ... args) {
		PreparedStatement pStatement = session.prepare(cql);
		return executeBoundStatement(createBoundSatement(pStatement, args));
	}
	
	/**
	 * Execute custom query with prepared statement
	 * @param cql
	 * @return
	 */
	public ResultSet selectWithKeys(String cf, Object ... args) {
		Statement query = QueryBuilder.select().all().from(cf).where(QueryBuilder.in("key", args)).enableTracing();		
		return session.execute(query);
	}
	
	public ResultSet deleteWithKeys(String cf, Object ... args) {
		Statement query = QueryBuilder.delete().from(cf).where(QueryBuilder.in("key", args)).enableTracing();		
		return session.execute(query);
	}
	
	public ResultSet getKey(Object id, String column, Class<?> clazz) {
		BoundStatement bStatement = indexQuery(id, column, clazz);
		return executeBoundStatement(bStatement);
	}
	
	public void insertWideRow(String cql, Object ... args){
		PreparedStatement pStatement = session.prepare(cql);
		executeBoundStatement(createBoundSatement(pStatement, args));
	}
	
//	public ResultSet updateWithKeys(String cql, String cf, String name, Object value, Object ... args) {
//		Statement query = QueryBuilder.update(cf).with(QueryBuilder.set(name, value)).where(QueryBuilder.in("key", args)).enableTracing();		
//		return session.execute(query);
//	}
	
	protected Session getSession() {
		return StorageDAO.session;
	}
	
	/**
	 * Shutdown session and cluster
	 */
	public void close() {
		if (session != null){
			session.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}
	
	/**
	 * Inserts data into given columnFamily
	 * @param obj
	 * @param columnFamily
	 * @throws Exception
	 */
	public <E> ResultSet put(Object obj, String columnFamily) throws Exception{
		BoundStatement bStatement = insertQuery(obj, columnFamily, Constants.DEFAULT_ZERO);
		return executeBoundStatement(bStatement);
	}
	
	/**
	 * Inserts data into given columnFamily with TTL
	 * @param obj
	 * @param columnFamily
	 * @throws Exception
	 */
	public <E> ResultSet put(Object obj, String columnFamily, int ttl) throws Exception{
		BoundStatement bStatement = insertQuery(obj, columnFamily, ttl);
		return executeBoundStatement(bStatement);
	}
	
	/**
	 * Queries columnFamily with given id
	 * @param id
	 * @param columnFamily
	 * @param clazz
	 * @throws Exception
	 */
	public <E> ResultSet get(Object id, String columnFamily, Class<E> clazz) {
		BoundStatement bStatement = getQuery(id, columnFamily, clazz);
		return executeBoundStatement(bStatement);
	}
	
	/**
	 * Execute query wrapped in BoundStatement
	 * @param statement
	 * @return
	 */
	protected ResultSet executeBoundStatement(BoundStatement statement){		
		return session.execute(statement.bind());	
	}
	
	/**
	 * Create BoundStatement from PreparedStatement and defined fields. Assumes fields are in order of statement.
	 * @param statement
	 * @param fields
	 * @return
	 */
	protected BoundStatement createBoundSatement(PreparedStatement statement, Object... fields){
		statement.setConsistencyLevel(consistencyLevel);
		return new BoundStatement(statement).bind(fields);
	}
	
	/**
	 * Create BoundStatement from prepared statement
	 * @param statement
	 * @return
	 */
	protected BoundStatement createBoundSatement(PreparedStatement statement){
		statement.setConsistencyLevel(consistencyLevel);
		return new BoundStatement(statement);
	}
		
	/**
	 * Build get query in cql3
	 * @param key
	 * @param columnFamily
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	protected <E> BoundStatement getQuery(Object key, String columnFamily, Class<E> clazz) {		
		StringBuilder getQueryBuilder = new StringBuilder("SELECT ");
		StringBuilder fromQueryBuilder = new StringBuilder(" FROM ");
		fromQueryBuilder.append(columnFamily);		
		fromQueryBuilder.append(" WHERE key=?;");		
		
		Field[] attributes = clazz.getDeclaredFields();		
		
		int i = 0;
		for(Field f : attributes){
			String attributeName = f.getName();
			getQueryBuilder.append(attributeName);
			if(attributes.length > ++i){				
				getQueryBuilder.append(", ");				
			}
		}
		
		String query = getQueryBuilder.append(fromQueryBuilder).toString();
		LOGGER.info(query.replace(SQLConstants.PREPARED_STATEMENT_FLAG, key.toString()));
		PreparedStatement pStatement = session.prepare(query);		
		return createBoundSatement(pStatement,key);	
	}
	
	protected <E> BoundStatement indexQuery(Object key, String column, Class<E> clazz) {		
		StringBuilder getQueryBuilder = new StringBuilder("SELECT ");
		getQueryBuilder.append(column);
		StringBuilder fromQueryBuilder = new StringBuilder(" FROM ");
		fromQueryBuilder.append(clazz.getSimpleName().toLowerCase()+"_index");		
		fromQueryBuilder.append(" WHERE key=?;");		
		
		String query = getQueryBuilder.append(fromQueryBuilder).toString();
		LOGGER.info(query.replace(SQLConstants.PREPARED_STATEMENT_FLAG, key.toString()));
		PreparedStatement pStatement = session.prepare(query);		
		return createBoundSatement(pStatement,key);	
	}
	
	/**
	 * Get TTL for specific column
	 * @param key
	 * @param columnFamily
	 * @param column
	 * @return
	 * @throws Exception
	 */
	protected <E> BoundStatement getTTL(Object key, String columnFamily, String column) {
		StringBuilder getQueryBuilder = new StringBuilder("SELECT ");
		getQueryBuilder.append(" TTL(");
		getQueryBuilder.append(column);
		getQueryBuilder.append(") ");
		getQueryBuilder.append(" FROM ");		
		getQueryBuilder.append(columnFamily);		
		getQueryBuilder.append(" WHERE key=");
		getQueryBuilder.append(key);
				
		String query = getQueryBuilder.toString();
		LOGGER.info(query.replace(SQLConstants.PREPARED_STATEMENT_FLAG, key.toString()));
		PreparedStatement pStatement = session.prepare(query);		
		return createBoundSatement(pStatement);	
	}
	
	/**
	 * Get TTL for all columns in column family
	 * @param key
	 * @param columnFamily
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	protected <E> BoundStatement getTTL(Object key, String columnFamily, Class<E> clazz){
		StringBuilder getQueryBuilder = new StringBuilder("SELECT ");
		StringBuilder fromQueryBuilder = new StringBuilder(" FROM ");		
		fromQueryBuilder.append(columnFamily);
		fromQueryBuilder.append(" WHERE key=");
		fromQueryBuilder.append(key);
		
		Field[] attributes = clazz.getDeclaredFields();		
		
		int i = 0;
		for(Field f : attributes){
			String attributeName = f.getName();			
			if(attributes.length > i){				
				getQueryBuilder.append(" TTL(");
				getQueryBuilder.append(attributeName);
				getQueryBuilder.append(")");
				if(attributes.length < ++i){
					getQueryBuilder.append(", ");
				}
			}
		}
		
		String query = getQueryBuilder.append(fromQueryBuilder).toString();
		LOGGER.info(query.replace(SQLConstants.PREPARED_STATEMENT_FLAG, key.toString()));
		PreparedStatement pStatement = session.prepare(query);		
		return createBoundSatement(pStatement);	
	}	
		
	/**
	 * Build insert query using cql3
	 * @param obj
	 * @param columnFamily
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	protected <E> BoundStatement insertQuery(Object obj, String columnFamily, Integer ttl) throws Exception{
		StringBuilder insertQueryBuilder = new StringBuilder("INSERT INTO ");
		insertQueryBuilder.append(obj.getClass().getSimpleName());
		insertQueryBuilder.append("(");
		StringBuilder valueQueryBuilder = new StringBuilder(" VALUES(");
		Field[] attributes = obj.getClass().getDeclaredFields();
		Object[] fields = new Object[attributes.length];
		try{
			int i = 0;
			for(Field f : attributes){
				f.setAccessible(true);
				String attributeName = f.getName();
				Object value =  f.get(obj);
				
				fields[i++]=value;
				if(attributes.length == i){
					insertQueryBuilder.append(attributeName);
					insertQueryBuilder.append(")");
					valueQueryBuilder.append("?)");
				}else{
					insertQueryBuilder.append(attributeName);
					insertQueryBuilder.append(", ");
					valueQueryBuilder.append("?, ");
				}
			}
		}catch(Exception e){
			throw new Exception(e.getMessage(),e);
		}
		String query = insertQueryBuilder.append(valueQueryBuilder).append(" USING TTL ").append(ttl).toString();
		LOGGER.info(query);
		PreparedStatement pStatement = session.prepare(query);		
		return createBoundSatement(pStatement, fields);	
	}
	
	/**
	 * Build insert query using cql3
	 * @param obj
	 * @param columnFamily
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	protected ResultSet createCFQuery(Class<?> clazz) throws Exception{
		StringBuilder insertQueryBuilder = new StringBuilder("CREATE COLUMNFAMILY ");
		insertQueryBuilder.append(clazz.getSimpleName());
		insertQueryBuilder.append("(");
		
		Field[] attributes = clazz.getDeclaredFields();
		
		try{
			int i = 0;
			for(Field f : attributes){
				f.setAccessible(true);
				String attributeName = f.getName();
				
				if(hasCassandraType(f.getType().getSimpleName())){
					insertQueryBuilder.append(attributeName);
					insertQueryBuilder.append(Constants.SPACE);
					insertQueryBuilder.append(toCassandraType(f.getType().getSimpleName()));
					insertQueryBuilder.append(Constants.SPACE); 
					if(attributeName.equals(Constants.KEY)){
						insertQueryBuilder.append(SQLConstants.P_KEY);
						insertQueryBuilder.append(SQLConstants.QUERY_DELIMITER);
					}
				}
				
				if(attributes.length == i){
					insertQueryBuilder.append(attributeName);
					insertQueryBuilder.append(")");
				}else{
					insertQueryBuilder.append(attributeName);
					insertQueryBuilder.append(SQLConstants.QUERY_DELIMITER);					
				}
			}
		}catch(Exception e){
			throw new Exception(e.getMessage(),e);
		}	
		LOGGER.info(insertQueryBuilder);
		return execute(insertQueryBuilder.toString());
	}
	
	
	private static String toCassandraType(String type){
		return CassandraTypes.fromJava(type).name().toLowerCase();		
	}
	
	private static boolean hasCassandraType(String type){
		return null != CassandraTypes.fromJava(type).name().toLowerCase();		
	}
	
	public static Object getData(Class<?> clazz, Row row, String field){
		switch(Constants.TYPES.get(clazz.getSimpleName().toLowerCase())){
		case Constants.TYPE_STRING :
			return row.getString(field);
		case Constants.TYPE_BOOLEAN : 
			return row.getBool(field);
		case Constants.TYPE_BYTE : 
			return row.getBytes(field);
		case Constants.TYPE_DATE : 
			return row.getDate(field);
		case Constants.TYPE_DOUBLE :
			return row.getDouble(field);
		case Constants.TYPE_INT :
			return row.getInt(field);
		case Constants.TYPE_INTEGER :
			return row.getInt(field);
		case Constants.TYPE_LONG :
			return row.getLong(field);
		case Constants.TYPE_SHORT :
			return row.getInt(field);
		case Constants.TYPE_UUID :
			return row.getUUID(field);	
		default : return null;
		}
	}
}
