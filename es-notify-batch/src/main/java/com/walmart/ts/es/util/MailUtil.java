package com.walmart.ts.es.util;

import java.net.InetAddress;
import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.log4j.Logger;

import com.walmart.ts.es.constants.Constants;
import com.walmart.ts.es.constants.PropertiesConstants;

public class MailUtil {
	
	private static final Logger LOGGER = Logger.getLogger(MailUtil.class);
	
	/*** sending an alert through email ***/
	public static boolean postMail(String subject, String message, String recipientAddress) {		 
		String emailHost = PropertyUtil.getInstance().getString(PropertiesConstants.EMAIL_HOST);				
		String []toAddress = recipientAddress.split(Constants.DELIMITER);		
		try{
			String fromAddress = InetAddress.getLocalHost().getHostName() +Constants.EMAIL_SUFFIX;
			
			// create some properties and get the default Session
			Properties 	property = new Properties();
			property.put(Constants.SMTP_HOST, emailHost);		
			
			Session session = Session.getDefaultInstance(property, null);
			session.setDebug(false);
			
			// create a message
			MimeMessage mimeMessage = new MimeMessage(session);
			mimeMessage.setFrom(new InternetAddress(fromAddress));    
			
			InternetAddress[] address = new InternetAddress[toAddress.length];
			for (int addressIndex = Constants.DEFAULT_ZERO; addressIndex < toAddress.length; addressIndex++) {       
				address[addressIndex] = new InternetAddress(toAddress[addressIndex]);
			}
			
			mimeMessage.setRecipients(Message.RecipientType.TO, address);
			mimeMessage.setSubject(subject);
			mimeMessage.setSentDate(new Date());		
			
			// create and fill the first message part
			MimeBodyPart mbp1 = new MimeBodyPart();
			mbp1.setContent(message, Constants.CONTENT);
			
			Multipart mp = new MimeMultipart();
			//MimeBodyPart mbp2 = new MimeBodyPart();
			
			// create the Multipart and its parts to it           
			mp.addBodyPart(mbp1);         
			
			// add the Multipart to the message
			mimeMessage.setContent(mp);
			
			// send the message
			Transport.send(mimeMessage);
			LOGGER.info("Message sent to "+recipientAddress);
		}catch(Exception e){
			LOGGER.error(e.getMessage());
		}
		return true;
	}
}
