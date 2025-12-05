package org.todaybook.bookpreprocessingworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BookpreprocessingworkerApplication {

	public static void main(String[] args) {
		SpringApplication.run(BookpreprocessingworkerApplication.class, args);
	}

}
