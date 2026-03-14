package com.evi_login_data_processor.evi_login_processor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class EviLoginProcessorApplicationTests {

	@Test
	void contextLoads() {
	}

}
