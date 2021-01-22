package com.amazon.gdpr.configuration;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Config files picks up the data source detail from application.properties file
 * This establishes the JDBC connection
 ****************************************************************************************/
@Configuration
public class DatabaseConfig {
	
	public static String CURRENT_CLASS		 		= GlobalConstants.CLS_DATABASECONFIG;
	
	/**
	 * The datasource for GDPR project is loaded
	 * @return datasource
	 */
	@Bean(name = "gdprDS")
	@ConfigurationProperties(prefix = "spring.datasource")
	public DataSource dataSource() {
		String CURRENT_METHOD = "dataSource";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		return DataSourceBuilder.create().build();
	}

	/**
	 * This method returns the JDBC Template
	 * @param ds - GDPR Datasource detail
	 * @return - JDBCTemplate
	 */
	@Bean(name = "gdprJdbcTemplate")
	public JdbcTemplate jdbcTemplate(@Qualifier("gdprDS") DataSource gdprDataSource) {
		String CURRENT_METHOD = "jdbcTemplate";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		return new JdbcTemplate(gdprDataSource);
	}
}