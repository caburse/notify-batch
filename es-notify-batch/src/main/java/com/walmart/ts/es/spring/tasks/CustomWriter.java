package com.walmart.ts.es.spring.tasks;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.constants.PropertiesConstants;
import com.walmart.ts.es.constants.SQLConstants;
import com.walmart.ts.es.dao.StorageDAO;
import com.walmart.ts.es.model.Alert;
import com.walmart.ts.es.model.DurationType;
import com.walmart.ts.es.util.MailUtil;
import com.walmart.ts.es.util.PropertyUtil;

public class CustomWriter implements ItemWriter<Alert> {
	private static final Logger LOGGER = Logger.getLogger(CustomWriter.class);
	
	@Autowired
	private StorageDAO storageDAO;
	
//	private static Map<String,Long> duration = new HashMap<String,Long>();
	/* 
	 * Sends Notification
	 * (non-Javadoc)
	 * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void write(List<? extends Alert> alerts) throws Exception {		
		for(int i = 0; i < alerts.size(); i++){		
			Collection<Alert> collection = (Collection<Alert>) alerts.get(i);
			for(Alert alert : collection){
				if(alertExists(alert.getSource())){
					continue;
				}
				
				//Calculate ok to send time
				Calendar cal = Calendar.getInstance();
				cal.set(DurationType.fromString(alert.getBlackoutDurationType()).getType(), 
						cal.get(
								DurationType.fromString(alert.getBlackoutDurationType()).getType()) 
								+
								alert.getBlackoutDuration());
				
	//			duration.put(alert.getSource(), cal.getTimeInMillis());
				storageDAO.execute(SQLConstants.INSERT_NOTIFICATION, alert.getSource(), cal.getTime(), PropertyUtil.getInstance().getInt(PropertiesConstants.NOTIFICATION_TTL));			
				MailUtil.postMail(alert.getSource()+" Logging Threshold exceeded", alert.getSource()+" exceeded Max Occurence of " + alert.getFieldValue() + " in " + alert.getFieldKey(), alert.getUserId()+"@wal-mart.com");
				LOGGER.info("Job: "+alert.getSource() + " exceeded threshold!");
			}
		}
	}
	
	/**
	 * Check to see if an alert was all ready sent and if so continue 
	 * @param source
	 * @return
	 */
	private boolean alertExists(String source){		
		ResultSet rs = storageDAO.execute(SQLConstants.GET_NOTIFICATION, source);
		List<Row> rows = rs.all();
		
		if(rows.size() > 0){
			Date date = rows.get(0).getDate(Constants.CREATE_DATE.toLowerCase());
			if(Calendar.getInstance().getTimeInMillis() < date.getTime()){
				return true;
			}
		}
		return false;
		//TODO fixing this as it sucks TEST!!!
//		return duration.containsKey(source) 
//				&& 
//				Calendar.getInstance().getTimeInMillis()  
//				<
//				duration.get(source);
	}
}