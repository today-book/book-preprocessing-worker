package org.todaybook.bookpreprocessingworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BookpreprocessingworkerApplication {

	/**
	 * 스프링 부트 애플리케이션을 시작한다.
	 *
	 * 애플리케이션 컨텍스트를 초기화하고 내장 서버를 기동한다.
	 *
	 * @param args 명령줄에서 전달된 애플리케이션 인수
	 */
	public static void main(String[] args) {
		SpringApplication.run(BookpreprocessingworkerApplication.class, args);
	}

}