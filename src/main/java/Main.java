import dao.CityDAO;
import dao.CountryDAO;
import domain.City;
import domain.Country;
import domain.CountryLanguage;
import io.lettuce.core.RedisClient;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.nonNull;

public class Main {

    private  final SessionFactory sessionFactory;
    //private final RedisClient redisClient;
   // private final ObjectMapper mapper;
    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {
        sessionFactory = prepareRelationalDb();

        countryDAO = new CountryDAO(sessionFactory);
        cityDAO = new CityDAO(sessionFactory,countryDAO);

//        redisClient = prepareRedisClient();
//        mapper = new ObjectMapper();
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

    public static void main(String[] args) {
        Main main = new Main();
        List<City> allCities = main.cityDAO.fetchData();
        main.shutdown();

    }

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
//        if (nonNull(redisClient)) {
//            redisClient.shutdown();
//        }
    }

}
