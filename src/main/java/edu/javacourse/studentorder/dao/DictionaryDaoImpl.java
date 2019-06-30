package edu.javacourse.studentorder.dao;

import edu.javacourse.studentorder.config.Config;
import edu.javacourse.studentorder.domain.CountryArea;
import edu.javacourse.studentorder.domain.PassportOffice;
import edu.javacourse.studentorder.domain.RegisterOffice;
import edu.javacourse.studentorder.domain.Street;
import edu.javacourse.studentorder.exception.DaoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class DictionaryDaoImpl implements DictionaryDao {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryDaoImpl.class);

    private static final String GET_STREET = "select street_code, street_name " +
            "from jc_street where upper(street_name) like upper(?)";

    private static final String GET_PASSPORT = "select * " +
            "from jc_passport_office where p_office_area_id = ?";

    private static final String GET_REGISTER = "select * " +
            "from jc_register_office where r_office_area_id = ?";
    private static final String GET_AREA = "select * " +
            "from jc_country_struct where area_id like ? and area_id <> ?";

    private Connection getConnection() throws SQLException {
        return ConnectionBuilder.getConnection();
    }

    public List<Street> findStreets(String pattern) throws DaoException {
        List<Street> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(GET_STREET)) {
            st.setString(1, "%" + pattern + "%");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                Street str = new Street(
                        rs.getLong("street_code"),
                        rs.getString("street_name")
                );
                result.add(str);
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }
        return result;
    }

    @Override
    public List<PassportOffice> findPassportOffices(String areaId) throws DaoException {
        List<PassportOffice> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(GET_PASSPORT)) {
            st.setString(1, areaId);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                PassportOffice passportOffice = new PassportOffice(
                        rs.getLong("p_office_id"),
                        rs.getString("p_office_area_id"),
                        rs.getString("p_office_name")
                );
                result.add(passportOffice);
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }
        return result;
    }

    @Override
    public List<RegisterOffice> findRegisterOffices(String areaId) throws DaoException {
        List<RegisterOffice> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(GET_REGISTER)) {
            st.setString(1, areaId);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                RegisterOffice passportOffice = new RegisterOffice(
                        rs.getLong("r_office_id"),
                        rs.getString("r_office_area_id"),
                        rs.getString("r_office_name")
                );
                result.add(passportOffice);
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }
        return result;
    }

    @Override
    public List<CountryArea> findAreas(String areaId) throws DaoException {
        List<CountryArea> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(GET_AREA)) {
            String param1 = buildParam(areaId);
            String param2 = areaId;
            st.setString(1, param1);
            st.setString(2, param2);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                CountryArea area = new CountryArea(
                        rs.getString("area_id"),
                        rs.getString("area_name")
                );
                result.add(area);
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }
        return result;
    }

    private String buildParam(String areaId) throws SQLException {
        String result = "";
        if (areaId == null || areaId.trim().isEmpty()) {
            result = "__0000000000";
        } else if (areaId.endsWith("0000000000")) {
            result = areaId.substring(0, 2) + "___0000000";
        } else if (areaId.endsWith("0000000")) {
            result = areaId.substring(0, 5) + "___0000";
        } else if (areaId.endsWith("0000")) {
            result = areaId.substring(0, 8) + "____";
        } else {
            throw new SQLException("Invalid parametr 'areaId': " + areaId);
        }
        return result;
    }
}
