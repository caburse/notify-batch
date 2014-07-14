package com.walmart.ts.es.model;

import java.util.Calendar;

public enum DurationType {
	SECONDS(Calendar.SECOND),
	MINUTES(Calendar.MINUTE),
	HOURS(Calendar.HOUR),
	DAYS(Calendar.DATE),
	MONTHS(Calendar.MONTH);
	
	private int type;
	private String description;
	
	private DurationType(int type){
		this.type=type;
	}
	private DurationType(String description){
		this.description=description;	
	}
		
	public static DurationType fromInt(int type) {
		switch(type){
			case Calendar.SECOND:
				return SECONDS;
			case Calendar.MINUTE:
				return MINUTES;
			case Calendar.HOUR:
				return HOURS;
			case Calendar.DATE:
				return DAYS;
			case Calendar.MONTH:
				return MONTHS;
			default :
				return null;
		}		
	 }
		
	public static DurationType fromString(String description) {		
		for (DurationType value : DurationType.values()) {			
			if (value.toString().equalsIgnoreCase(description)) {
				return value;
			}
		}		 
		 return null;
	 }
	
	public int getType(){
		return this.type;
	}
	public String getDescription(){
		return this.description;
	}
}
