import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import com.fasterxml.jackson.databind.ObjectMapper;

import mysql.dao.CityDAO;
import mysql.dao.CountryDAO;
import mysql.domain.City;
import mysql.domain.Country;
import mysql.domain.CountryLanguage;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import redis.dao.CityCountryDAO;
import redis.domain.CityCountry;

import java.util.List;
import java.util.Properties;

import static java.util.Objects.nonNull;

public class Main {

    private  final SessionFactory sessionFactory;
    private final RedisClient redisClient;
     private final ObjectMapper mapper;
    private final CityDAO cityDAO;

    private final CityCountryDAO cityCountryDAO;

    public Main() {
        sessionFactory = prepareRelationalDb();

        CountryDAO countryDAO = new CountryDAO(sessionFactory);
        cityDAO = new CityDAO(sessionFactory, countryDAO);
        cityCountryDAO=new CityCountryDAO();

        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }
    private SessionFactory prepareRelationalDb() {
        final SessionFactory sessionFactory;
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3306/project_4db");
        properties.put(Environment.USER, "admin");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");
        properties.put(Environment.STATEMENT_BATCH_SIZE, "100");

        sessionFactory = new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
        return sessionFactory;
    }
    private RedisClient prepareRedisClient() {
        RedisClient redisClient = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            System.out.println("\nConnected to Redis\n");
        }
        return redisClient;
    }

    public static void main(String[] args) {
        Main main = new Main();

        List<City> allCities = main.cityDAO.fetchData();
        List<CityCountry> preparedData = main.cityCountryDAO.transformData(allCities);
        main.cityCountryDAO.pushToRedis(preparedData,main.redisClient,main.mapper);

        main.sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        main.cityCountryDAO.testRedisData(ids,main.redisClient,main.mapper);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.cityDAO.testMysqlData(ids);
        long stopMysql = System.currentTimeMillis();

        System.out.printf("%s:\t%d ms\n", "Redis", (stopRedis - startRedis));
        System.out.printf("%s:\t%d ms\n", "MySQL", (stopMysql - startMysql));
        main.shutdown();


    }



    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

}
