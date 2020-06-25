package com.home.kafkatest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import com.home.kafkatest.model.TestData;

@Service
public class TestSenderService {

	private final KafkaTemplate<Long, TestData> kafkaTemplate;
	private final ObjectMapper objectMapper;

	@Autowired
	public TestSenderService(KafkaTemplate<Long, TestData> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	@Scheduled(initialDelay = 10000, fixedDelay = 5000)
	public void produce() {
		TestData td = new TestData((long) 1, LocalDateTime.now(), LocalDateTime.now(), "Test", "tesT");
		try {
			System.out.println("<= sending" + objectMapper.writeValueAsString(td));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		kafkaTemplate.send("test", td);
	}

}
