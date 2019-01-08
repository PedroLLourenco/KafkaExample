package com.gaurav.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.gaurav.kafka.constants.EmailConstants;
import com.gaurav.kafka.constants.IKafkaConstants;
import com.gaurav.kafka.consumer.ConsumerCreator;
import com.gaurav.kafka.pojo.EmailTryingDTO;
import com.google.gson.Gson;

public class App {
	public static void main(String[] args) {
		runConsumer();
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

        	System.out.println("Consumer: I am up");
			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
				
				String[] myJson = record.value().split("####");
				System.out.println("Email: " + myJson[1]);
				EmailTryingDTO dto = new Gson().fromJson(myJson[1], EmailTryingDTO.class);
			
			
			System.out.println("tentando");
			try {
				mailLikeAS(EmailConstants.FTP_HOST,
						EmailConstants.FTP_FROM,
						EmailConstants.FTP_PASSWORD,
						dto.getTo(),
						dto.getSubject(),
						dto.getBody());
	        	System.out.println("Consumer: Deu bom");
	        } catch (Exception e) {
	        	System.out.println("Consumer: Nao deu bom");
	    	}
			});
				consumer.commitAsync();
			}
		consumer.close();
	}

	
	public static void mailLikeAS(String host, String from, String password, String to, String subject, String body) {
		String[] novoTo = getMailToArray(to);
		mailLikeAS(host, from, password, novoTo, subject, body, null);
	}

	
	public static void mailLikeAS(String host, String from, String password, String[] to, String subject, String body, File file) {
		mailLikeAS(host, from, password, to, subject, body, file, null);
	}
	
	private  static void mailLikeAS(String host, String from, String password, String[] to, String subject, String body, File file, String[] blindCopy) {
		try {
            
			// Get system properties
			Properties props = new Properties();

			// Setup mail server
			props.put("mail.smtp.host", host);

			props.put("mail.smtp.user", from);

			SMTPAuthenticator authenticator = null;
			if(password != null) {
				props.put("mail.smtp.auth", "true");
				authenticator = new SMTPAuthenticator();
				authenticator.username = from;
				authenticator.password = password;
			}
			// Get session
			Session session = Session.getInstance(props, authenticator);

			// Define message
			Message message = new MimeMessage(session);

			// Set the from address
			message.setFrom(new InternetAddress(from));

			// Set the to address
			if(to != null && to.length > 0) {	
				for (int contador = 0; contador < to.length; contador++) {
					message.addRecipient(Message.RecipientType.TO, new InternetAddress(to[contador]));
				}
			}
			
			// Set the subject
			message.setSubject(subject);

			// Create the message part 
			BodyPart messageBodyPart = new MimeBodyPart();
			// Fill the message
			messageBodyPart.setText(body);

			// Create a Multipart
			Multipart multipart = new MimeMultipart();

			// Add part one
			multipart.addBodyPart(messageBodyPart);
			
			//HTML
			if(body.toLowerCase().indexOf("<html>") >= 0) {
				messageBodyPart.setContent(body, "text/html");
				//messageBodyPart.setDataHandler(new DataHandler());
			}

			// Put parts in message
			message.setContent(multipart);

			

			if(password == null) {
				// Send the message
				Transport.send(message);
			} else {
				message.saveChanges(); // implicit with send()
				Transport transport = session.getTransport("smtp");
				transport.connect(host, from, password);
				transport.sendMessage(message, message.getAllRecipients());
				transport.close();
			}

		} catch(javax.mail.internet.AddressException e) {
			e.printStackTrace();
			System.out.println("DEU A PRIMEIRA EXCEPTION");

		} catch(MessagingException e) {
			e.printStackTrace();
			System.out.println("DEU A SEGUNDA EXCEPTION");
		}
	}
	
	private static String[] getMailToArray(String to) {
		ArrayList lista = new ArrayList();
		StringTokenizer strToken = new StringTokenizer(to, ";");
		while(strToken.hasMoreTokens()) {
			String s = strToken.nextToken().trim();
			lista.add(s);
		}

		String[] array = (String[]) lista.toArray(new String[lista.size()]);

		return array;
	}
	
	
	static class SMTPAuthenticator extends Authenticator {
		
		public String username;
		
		public String password;
		
		public PasswordAuthentication getPasswordAuthentication() {
			return new PasswordAuthentication(username, password);
		}
	}
}
