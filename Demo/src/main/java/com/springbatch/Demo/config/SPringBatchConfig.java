package com.springbatch.Demo.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.springbatch.Demo.batch.processor.StudentProcessor;
import com.springbatch.Demo.batch.processor.UserProcessor;
import com.springbatch.Demo.model.Student;
import com.springbatch.Demo.model.User;
import com.springbatch.Demo.sql.StudentPreparedStatementSetter;

@Configuration
@EnableBatchProcessing
public class SPringBatchConfig {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;


//	//-----------------------------------Job 1--------------------------------------------------------------


	@Bean("job1")
	@Primary
	@Qualifier("job1")
	public Job job1(JobBuilderFactory jobBuilderFactory, 
			StepBuilderFactory stepBuilderFactory, 
			ItemReader<User> itemReader1,
			ItemProcessor<User, User> itemProcessor1,
			ItemWriter<User> itemWriter1) {

		Step step = stepBuilderFactory.get("ETL-file-Load")
				.<User, User>chunk(10)
				.reader(itemReader1)
				.processor(itemProcessor1)
				.writer(itemWriter1)
				.build();

		return jobBuilderFactory.get("ETL-Load")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();
	}

	@Bean
	public FlatFileItemReader<User> itemReader1(){
		FlatFileItemReader<User> itemReader1 = new FlatFileItemReader<>();
		itemReader1.setResource(new ClassPathResource("user.csv"));
		itemReader1.setName("CSV-Reader");
		itemReader1.setLinesToSkip(1);
		itemReader1.setLineMapper(lineMapper());
		return itemReader1;

	}

	@Bean
	public LineMapper<User> lineMapper() {
		DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();

		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames(new String[] {"userId", "name", "address"});

		BeanWrapperFieldSetMapper<User> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
		beanWrapperFieldSetMapper.setTargetType(User.class);

		defaultLineMapper.setLineTokenizer(lineTokenizer);
		defaultLineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);

		return defaultLineMapper;
	}

	@Bean
	public ItemProcessor<User, User> itemProcessor1() {
		return new UserProcessor();
	}


	@Bean
	public JdbcBatchItemWriter<User> itemWriter1() {
		JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<User>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		writer.setSql("INSERT INTO users (userId,name,address) " +
				"VALUES (:userId, :name,:address)");
		writer.setDataSource(dataSource);
		return writer;
	}

//--------------------------------------Job 2------------------------------------------------------------------------	
	
	@Bean("job2")
	@Qualifier("job2")
	public Job job2(JobBuilderFactory jobBuilderFactory, 
			StepBuilderFactory stepBuilderFactory, 
			ItemStreamReader<Student> itemReader2,
			ItemProcessor<Student, Student> itemProcessor2,
			ItemWriter<Student> itemWriter2) {

		Step step = stepBuilderFactory.get("Update-Students-Step")
				.<Student, Student>chunk(10)
				.reader(itemReader2)
				.processor(itemProcessor2)
				.writer(itemWriter2)
				.build();

		return jobBuilderFactory.get("Update-Students-Job")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();
	}

	@Bean
	public ItemStreamReader<Student> itemReader2() {
		System.out.println("Inside Reader");
		JdbcCursorItemReader<Student> reader = new JdbcCursorItemReader<Student>();
		reader.setDataSource(dataSource);
		reader.setSql("select studentId, name, address from students");
		reader.setRowMapper(new StudentRowMapper());
		return reader;
	}

	@Component
	public class StudentRowMapper implements RowMapper<Student> {
		@Override
		public Student mapRow(ResultSet rs, int rowNum) throws SQLException {
			Student student = new Student();
			student.setStudentId(rs.getInt("studentId"));
			student.setName(rs.getString("Name"));
			student.setAddress(rs.getString("Address"));
			return student;
		}
	}

	@Bean
	public ItemProcessor<Student, Student> itemProcessor2() {
		System.out.println("Inside processor");
		return new StudentProcessor();
	}


	private static final String QUERY_UPDATE_STUDENTS = "UPDATE students SET name= ?, address = ? where studentId = ?";

	@Bean
	public ItemWriter<Student> itemWriter2(DataSource dataSource, NamedParameterJdbcTemplate jdbcTemplate) {
		System.out.println("Inside writer");
		JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		writer.setJdbcTemplate(jdbcTemplate);
		writer.setDataSource(dataSource);
		writer.setSql(QUERY_UPDATE_STUDENTS);

		ItemPreparedStatementSetter<Student> valueSetter = new StudentPreparedStatementSetter();
		writer.setItemPreparedStatementSetter(valueSetter);

		return writer;
	}

	

}