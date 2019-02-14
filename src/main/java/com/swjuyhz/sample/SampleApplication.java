package com.swjuyhz.sample;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
//import com.google.gs/**/on.Gson;
import com.swjuyhz.sample.sparkstream.ApplicationStartup;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
//@PropertySource({"classpath:application.properties"})
public class SampleApplication {
	
	public static void main(String[] args) {
		SpringApplication springApplication = new SpringApplication(SampleApplication.class);
		springApplication.addListeners(new ApplicationStartup());
		springApplication.run(args);
	}
	
//	//将Gson划归为spring管理
//	@Bean
//	public Gson gson() {
//		return new Gson();
//	}
}
