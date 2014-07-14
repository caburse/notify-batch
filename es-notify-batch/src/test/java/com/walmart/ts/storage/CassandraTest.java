package com.walmart.ts.storage;

import java.util.Calendar;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.walmart.ts.es.constants.PropertiesConstants;
import com.walmart.ts.es.constants.SQLConstants;
import com.walmart.ts.es.dao.StorageDAO;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.util.MapperUtil;
import com.walmart.ts.es.util.PropertyUtil;

public class CassandraTest {

	private StorageDAO storageDAO;
	
	@Before
	public void init(){
//		storageDAO = new StorageDAO("", null);		
		storageDAO = new StorageDAO("localhost",
				PropertyUtil.getInstance().getString(PropertiesConstants.KEYSPACE));
	}
	
//	@Test
//	public void testDropCF() throws Exception{
//		storageDAO.execute("DROP TABLE " + Alert.class.getSimpleName());
//	}	
//	@Test
//	public void testDropCF_index() throws Exception{
//		storageDAO.execute("DROP TABLE " + Alert.class.getSimpleName()+"_index");
//	}	
//	@Test
//	public void testCreateKeyspace() throws Exception{		
//		storageDAO.execute(String.format(SQLConstants.CREATE_KEYSPACE,1,1));	
//	}
//	@Test
//	public void testCreateColumnFamily() throws Exception{
//		storageDAO.execute(SQLConstants.CREATE_ALERT_CF);
//	}
//	@Test
//	public void testCreateColumnFamilyIndex() throws Exception{
//		storageDAO.execute(SQLConstants.CREATE_ALERT_CF_INDEX);
//	}	
//	@Test
//	public void insertRow() throws Exception {
//		Alert alert = getAlert();
//		storageDAO.put(alert, Alert.class.getSimpleName());
//		storageDAO.insertWideRow(
//				SQLConstants.INSERT_ALERT_INDEX, 
//				alert.getSource(), 
//				alert.getCreateDate(), 
//				alert.getKey());
//	}
	
	@Test
	public void getData() throws Exception{	
		
//		ResultSet irs = storageDAO.execute("select fkey from alert_index where source = 'initial';");
		ResultSet irs = storageDAO.getKey("initial", "fkey", Alert.class);
		
		List<Row> iRows = irs.all();
		
		for(Row iRow : iRows){
//			UUID id = iRow.getUUID("fkey");
			String id = iRow.getString("fkey");
			ResultSet ars = storageDAO.get(id, Alert.class.getSimpleName(), Alert.class);
			Alert alert = MapperUtil.map(ars, Alert.class);
			System.out.println(alert);			
		}
	}
		
	private Alert getAlert(){
		Alert alert = new Alert();
//		alert.setKey(UUIDs.timeBased());		
		alert.setTeamId("ISDAPSDE51");
		alert.setUserId("caburse");
		alert.setCron("*/5 * * * * ?");
		alert.setBlackoutDurationType("MINUTES");
		alert.setBlackoutDuration(5);
		alert.setSource("initial");
		alert.setFieldKey("level");
		alert.setFieldValue("ERROR");
		alert.setMaxOccurance(5);
		alert.setAlertDurationType("MINUTES");
		alert.setAlertDuration(5);
		alert.setCreateDate(Calendar.getInstance().getTime());
		alert.setKey(alert.getSource());
		return alert;
	}
}
