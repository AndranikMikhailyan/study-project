package edu.javacourse.studentorder.dao;


import edu.javacourse.studentorder.domain.CountryArea;
import edu.javacourse.studentorder.domain.PassportOffice;
import edu.javacourse.studentorder.domain.RegisterOffice;
import edu.javacourse.studentorder.domain.Street;
import edu.javacourse.studentorder.exception.DaoException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;

public class DictionaryDaoImplTest {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryDaoImplTest.class);

    @BeforeClass
    public static void startUp() throws Exception {
        DBInit.startUp();
    }

    @Test
    public void testStreet() throws DaoException {
        LocalDateTime dt1 = LocalDateTime.now();
        LocalDateTime dt2 = LocalDateTime.now();
        logger.info("TEST {} {}", dt1, dt2);
        List<Street> d = new DictionaryDaoImpl().findStreets("Про");
        Assert.assertTrue(d.size() == 2);
    }

    @Test
    public void testPassportOffice() throws DaoException {
        List<PassportOffice> po = new DictionaryDaoImpl().findPassportOffices("010020000000");
        Assert.assertTrue(po.size() == 2);
    }

    @Test
    public void testRegisterOffice() throws DaoException {
        List<RegisterOffice> ro = new DictionaryDaoImpl().findRegisterOffices("010010000000");
        Assert.assertTrue(ro.size() == 2);
    }

    @Test
    public void testArea() throws DaoException {
        List<CountryArea> co1 = new DictionaryDaoImpl().findAreas("");
        Assert.assertTrue(co1.size() == 2);
        List<CountryArea> co2 = new DictionaryDaoImpl().findAreas("020000000000");
        Assert.assertTrue(co2.size() == 2);
        List<CountryArea> co3 = new DictionaryDaoImpl().findAreas("020010000000");
        Assert.assertTrue(co3.size() == 2);
        List<CountryArea> co4 = new DictionaryDaoImpl().findAreas("020010010000");
        Assert.assertTrue(co4.size() == 2);
    }
}