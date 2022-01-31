package com.example.demo.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.example.demo.model.User;
import com.example.demo.repository.UserRepository;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
	
	@Autowired
	private DataSource datasource; //to declare the source i.e. in application.properties
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory; //to build a job
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory; //to build a step
	
	@Bean
	public FlatFileItemReader<User> reader(){ 
		FlatFileItemReader<User> reader = new FlatFileItemReader<>(); //creating a reader i.e. FlatFileItemReader<User> which extends ItemReader
		reader.setResource(new ClassPathResource("records.csv")); //to set source of data
		reader.setLineMapper(getLineMapper());
		reader.setLinesToSkip(1);
		return reader;
	}

	private LineMapper<User> getLineMapper() {
		DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>(); //sub class of line mapper
		
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(); //for setting field names
		tokenizer.setDelimiter(",");
		tokenizer.setNames(new String[] {"id", "prefix", "firstname", "lastname"});
		tokenizer.setIncludedFields(new int[] {0 , 1, 2, 3});
		
		BeanWrapperFieldSetMapper<User> fieldSetter = new BeanWrapperFieldSetMapper<>(); //for providing fields
		fieldSetter.setTargetType(User.class);
		
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldSetter);
		return lineMapper;
	}

	@Bean
	public UserItemProcessor processor() {
		return new UserItemProcessor();
	}
	
	@Bean
	public UserItemWriter writer() {
        return new UserItemWriter();
	}
	
	@Bean
	public Job job() {
		return this.jobBuilderFactory.get("USER-IMPORT-JOB")
				.incrementer(new RunIdIncrementer())
				.start(step1())
				.build();
	}
	
	@Bean
	public Step step1() {
		
		return this.stepBuilderFactory.get("USER-IMPORT-STEP")
				.<User, User>chunk(10)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
		
	}
}
