package dao;


import domain.City;
import domain.Country;
import lombok.AllArgsConstructor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class CityDAO {

    private final SessionFactory sessionFactory;
    private final CountryDAO countryDAO;

    public List<City> getItems(int offset, int limit) {
        Query<City> query=sessionFactory.getCurrentSession()
                .createQuery("select c from City c", City.class);
        query.setFirstResult(offset);
        query.setMaxResults(limit);
        return query.list();
    }

    public int getTotalCount() {
        Query<Long> query=sessionFactory.getCurrentSession()
                .createQuery("select count(c) from City c", Long.class);
        return Math.toIntExact(query.uniqueResult());
    }

    public List<City> fetchData() {

        try (Session session = sessionFactory.getCurrentSession()) {
            List<City> allCities = new ArrayList<>();
            session.beginTransaction();
            List<Country> countries=countryDAO.getAll();
            int totalCount = getTotalCount();
            int step = 500;
            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(getItems(i, step));
            }
            session.getTransaction().commit();
            return allCities;
        }

    }
}
