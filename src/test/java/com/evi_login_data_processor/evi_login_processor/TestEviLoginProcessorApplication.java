package com.evi_login_data_processor.evi_login_processor;

import org.springframework.boot.SpringApplication;

public class TestEviLoginProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.from(EviLoginProcessorApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
