
import connection.ConnectionManager;
import io.lettuce.core.RedisClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import mysql.dao.CityDAO;
import mysql.dao.CountryDAO;
import mysql.domain.City;

import org.hibernate.SessionFactory;

import redis.dao.CityCountryDAO;
import redis.domain.CityCountry;

import java.util.List;


import static java.util.Objects.nonNull;

public class Main {

    private  final SessionFactory sessionFactory;
    private final RedisClient redisClient;
     private final ObjectMapper mapper;
    private final CityDAO cityDAO;

    private final CityCountryDAO cityCountryDAO;

    public Main() {
        ConnectionManager connectionManager = new ConnectionManager();
        sessionFactory = connectionManager.createMySQLSessionFactory();

        CountryDAO countryDAO = new CountryDAO(sessionFactory);
        cityDAO = new CityDAO(sessionFactory, countryDAO);
        cityCountryDAO=new CityCountryDAO();

        redisClient = connectionManager.createRedisClient();
        mapper = new ObjectMapper();
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
