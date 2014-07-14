package com.walmart.ts.es;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App {
	
	/**
	 * Spring-batch will kick project off.
	 * @param args
	 */
	@SuppressWarnings({ "resource", "unused" })
	public static void main(String[] args) {		
		ApplicationContext context = new ClassPathXmlApplicationContext(args[0]);
	}
}
