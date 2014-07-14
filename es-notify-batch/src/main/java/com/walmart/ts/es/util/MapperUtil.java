package com.walmart.ts.es.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.beanutils.PropertyUtilsBean;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.walmart.ts.es.dao.StorageDAO;
import com.walmart.ts.es.model.Alert;

public class MapperUtil {

	public static <E> E map(ResultSet rs, Class<E> clazz) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
		List<Row> rows = rs.all();
		
		PropertyUtilsBean bUtil = new PropertyUtilsBean();
		
		E entity = clazz.newInstance();
		for(Row aRow : rows){				
			Field[] attributes = Alert.class.getDeclaredFields();
			for(Field f : attributes){
				String attributeName = f.getName();			
				Object attributeValue = StorageDAO.getData(f.getType(), aRow, attributeName);
				bUtil.setProperty(entity, attributeName, attributeValue);
			}
		}
		return entity;
	}
}