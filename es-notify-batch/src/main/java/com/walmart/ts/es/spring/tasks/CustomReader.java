package com.walmart.ts.es.spring.tasks;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.dao.StorageDAO;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.util.MapperUtil;

public class CustomReader implements ItemReader<List<Alert>> {

	@Autowired
	private StorageDAO storageDAO;
	/* 
	 * 
	 * (non-Javadoc)
	 * @see org.springframework.batch.item.ItemReader#read()
	 */
	@Override
	public List<Alert> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		List<Alert> alerts = new ArrayList<Alert>();
		
		ResultSet irs = storageDAO.getKey(
				Constants.RULE_KEY,
				Constants.FORIEGN_KEY,
				Alert.class);
		
		List<Row> iRows = irs.all();
		
		for(Row iRow : iRows){
			String id = iRow.getString(Constants.FORIEGN_KEY);
			ResultSet ars = storageDAO.get(id, Alert.class.getSimpleName(), Alert.class);
			alerts.add(MapperUtil.map(ars, Alert.class));			
		}
	
		return alerts;
	}
}