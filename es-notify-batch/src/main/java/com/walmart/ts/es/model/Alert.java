package com.walmart.ts.es.model;

import java.util.Date;


public class Alert {

	/** Primary Key **/
	private String key;	
	/** Remedy ID of issue owner **/
	private String teamId;
	/** Outlook id of user who created rule **/
	private String userId;
	/** Cron string for scheduled kickoff **/
	private String cron;
	/** Duration type of alert duration HOURS, MINUTES, SECONDS, etc... **/
	private String blackoutDurationType;
	/**  Duration to wait before sending another alert (if issue still exists) **/
	private int blackoutDuration;
	
	/** Application ID as defined in flume **/
	private String source;
	/** Field key used for search query **/
	private String fieldKey;
	/** Value to associate with field key for search query **/
	private String fieldValue;
	/** Max occurrences before alert **/
	private int maxOccurance;
	/** Duration type of alert duration HOURS, MINUTES, SECONDS, etc... **/
	private String alertDurationType;
	/** Duration to wait before alerting **/
	private int alertDuration;
	
	/** Create Date **/
	private Date createDate;
	
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getTeamId() {
		return teamId;
	}
	public void setTeamId(String teamId) {
		this.teamId = teamId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	}
	public String getBlackoutDurationType() {
		return blackoutDurationType;
	}
	public void setBlackoutDurationType(String blackoutDurationType) {
		this.blackoutDurationType = blackoutDurationType;
	}
	public int getBlackoutDuration() {
		return blackoutDuration;
	}
	public void setBlackoutDuration(int blackoutDuration) {
		this.blackoutDuration = blackoutDuration;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getFieldKey() {
		return fieldKey;
	}
	public void setFieldKey(String fieldKey) {
		this.fieldKey = fieldKey;
	}
	public String getFieldValue() {
		return fieldValue;
	}
	public void setFieldValue(String fieldValue) {
		this.fieldValue = fieldValue;
	}
	public int getMaxOccurance() {
		return maxOccurance;
	}
	public void setMaxOccurance(int maxOccurance) {
		this.maxOccurance = maxOccurance;
	}
	public String getAlertDurationType() {
		return alertDurationType;
	}
	public void setAlertDurationType(String alertDurationType) {
		this.alertDurationType = alertDurationType;
	}
	public int getAlertDuration() {
		return alertDuration;
	}
	public void setAlertDuration(int alertDuration) {
		this.alertDuration = alertDuration;
	}	
	public Date getCreateDate() {
		return createDate;
	}
	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}
	@Override
	public String toString() {
		return "Alert [key=" + key + ", teamId=" + teamId + ", userId="
				+ userId + ", cron=" + cron + ", blackoutDurationType="
				+ blackoutDurationType + ", blackoutDuration="
				+ blackoutDuration + ", source=" + source + ", fieldKey="
				+ fieldKey + ", fieldValue=" + fieldValue + ", maxOccurance="
				+ maxOccurance + ", alertDurationType=" + alertDurationType
				+ ", alertDuration=" + alertDuration + ", createDate="
				+ createDate + "]";
	}
		
}
