package com.home.kafkatest.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.home.kafkatest.model.TestData;

@Service

public class TestService {

	private final KafkaTemplate<Long, TestData> kafkaTemplate;
	private final ObjectMapper objectMapper;

	@Autowired
	public TestService(KafkaTemplate<Long, TestData> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	public void send(TestData td) {
		kafkaTemplate.send("test", td);
	}

	@KafkaListener(id = "Test", topics = { "test" }, containerFactory = "singleFactory")
	public void consume(TestData td) {
		try {
			System.out.println("=> receiving " + objectMapper.writeValueAsString(td));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}

}
