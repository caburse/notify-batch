package com.walmart.ts.es.constants;

import java.util.HashMap;
import java.util.Map;

public class Constants {

	/** Elastic Search **/
	public static final String CLUSTER_NAME="cluster.name";
	public static final String CLIENT_TRANSPORT="client.transport.sniff";	
	public static final String FIELD_SOURCE = "source";
	public static final String FIELD_TIMESTAMP = "@timestamp";
	public static final String FIELD_HOST = "host";
	public static final String ES_DATE_FORMAT = "yyyy-MM-ddTHH:mm:ss.SSS";
	
	/** Environment Properties **/
	public static final String PROPERTY_PREFIX = "lab_";
	public static final String PROPERTY_POSTFIX = ".properties";
	public static final String DEFAULT_PROPERTY = "lab_local.properties";
	public static final String ENVIRONMENT = "env";
	public static final Object SPACE = " ";
	
	/** eMail **/
	public static final String SMTP_HOST = "mail.smtp.host";
	public static final String EMAIL_SUFFIX = "@cb.com";
	public static final String CONTENT = "text/html";
	public static final String DELIMITER = ",";
	public static final int DEFAULT_ZERO = 0;
	
	/** Cassandra **/
	public static final String KEY = "key";
	public static final String RULE_KEYS = "initial";
	public static final String FORIEGN_KEY = "fkey";
	public static final String CREATE_DATE = "createDate";
	
	public static final Map<String, Integer> TYPES = new HashMap<String, Integer>();	
	static {
		TYPES.put("string", 1);
		TYPES.put("int", 2);
		TYPES.put("integer", 3);
		TYPES.put("boolean", 4);
		TYPES.put("long", 5);
		TYPES.put("double", 6);
		TYPES.put("byte", 7);
		TYPES.put("short", 8);
		TYPES.put("date", 9);
		TYPES.put("uuid", 10);
	}
	public static final int TYPE_STRING = 1;
	public static final int TYPE_INT = 2;
	public static final int TYPE_INTEGER = 3;
	public static final int TYPE_BOOLEAN = 4;
	public static final int TYPE_LONG = 5;
	public static final int TYPE_DOUBLE = 6;
	public static final int TYPE_BYTE = 7;
	public static final int TYPE_SHORT = 8;
	public static final int TYPE_DATE = 9;
	public static final int TYPE_UUID = 10;
}
