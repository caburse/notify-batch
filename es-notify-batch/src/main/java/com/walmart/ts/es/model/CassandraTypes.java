package com.walmart.ts.es.model;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

public enum CassandraTypes {	  
	 ASCII("string"),
     BIGINT("biginteger"),
     BLOB("object"),
     BOOLEAN("boolean"),     
     DECIMAL("float"),
     DOUBLE("double"),
     FLOAT("float"),
     INET("string"),
     INT("int"),     
     COUNTER("integer"),
     TEXT("string"),
     TIMESTAMP("date"),
     TIMEUUID("uuid"),
     UUID("uuid"),
     VARCHAR("string"),
     VARINT("integer");
     
	 private String type;
	 
	 private CassandraTypes(String type) {
	   this.type = type;
	 }
	 
	 public String getType() {
	   return type;
	 }
	 
	 public static CassandraTypes fromJava(String text) {
		 if (text != null) {
			 for (CassandraTypes value : CassandraTypes.values()) {
				 if (text.equalsIgnoreCase(value.getType())) {
					 return value;
				 }
			 }
		 }
		 return null;
	 }
	 public static Class<?> parse(String type){
		 if (StringUtils.isEmpty(type)) {
			 return null;
		 }
		 if(CASSANDRA_TYPES.containsKey(type)){
			 return CASSANDRA_TYPES.get(type);
		 }else {
			 return null;
		 }
		 
	 }
	 
	 /** Map of simple data types.**/
		public static final Map<String,Class<?>> CASSANDRA_TYPES = new HashMap<String,Class<?>>();	
		static {
			CASSANDRA_TYPES.put("ascii",String.class);
			CASSANDRA_TYPES.put("text",String.class);
			CASSANDRA_TYPES.put("varchar",String.class);
			CASSANDRA_TYPES.put("inet",String.class);
			CASSANDRA_TYPES.put("int",Integer.class);
			CASSANDRA_TYPES.put("counter",Integer.class);
			CASSANDRA_TYPES.put("varint",Integer.class);
			CASSANDRA_TYPES.put("uuid",UUID.class);
			CASSANDRA_TYPES.put("timeuuid",UUID.class);			
			CASSANDRA_TYPES.put("float",Float.class);
			CASSANDRA_TYPES.put("decimal",Float.class);
			CASSANDRA_TYPES.put("double",Double.class);
			CASSANDRA_TYPES.put("boolean",Boolean.class);
			CASSANDRA_TYPES.put("blob",Object.class);
			CASSANDRA_TYPES.put("bigint",BigInteger.class);			
		}	 
}