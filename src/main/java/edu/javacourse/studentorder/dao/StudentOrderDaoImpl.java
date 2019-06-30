package edu.javacourse.studentorder.dao;

import edu.javacourse.studentorder.config.Config;
import edu.javacourse.studentorder.domain.*;
import edu.javacourse.studentorder.exception.DaoException;
import org.postgresql.core.SqlCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StudentOrderDaoImpl implements StudentOrderDao {

    private static final Logger logger = LoggerFactory.getLogger(StudentOrderDaoImpl.class);

    private static final String INSERT_ORDER = "INSERT INTO jc_student_order(" +
            "student_order_status, student_order_date, " +
            "h_sur_name, h_given_name, h_patronymic, h_date_of_birth, h_passport_seria, h_passport_number, h_passport_date, " +
            "h_passport_office_id, h_post_index, h_street_code, h_building, h_extension, h_appartment, " +
            "h_university_id, h_student_number," +
            "w_sur_name, w_given_name, w_patronymic, w_date_of_birth, w_passport_seria, w_passport_number, w_passport_date, " +
            "w_passport_office_id, w_post_index, w_street_code, w_building, w_extension, " +
            "w_appartment, w_university_id, w_student_number, " +
            "certificate_id, register_office_id, marriage_date) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_CHILD = "INSERT INTO jc_student_child(" +
            "student_order_id, c_sur_name, c_given_name, c_patronymic, c_date_of_birth, " +
            "c_certificate_number, c_certificate_date, c_register_office_id, c_post_index, c_street_code, " +
            "c_building, c_extension, c_appartment) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ORDERS =
            "select so.*, ro.r_office_area_id, ro.r_office_name, " +
                    "po_h.p_office_area_id as h_p_office_area_id, po_h.p_office_name as h_p_office_name, " +
                    "po_w.p_office_area_id as w_p_office_area_id, po_w.p_office_name as w_p_office_name " +
                    "from jc_student_order so " +
                    "inner join jc_register_office ro on ro.r_office_id = so.register_office_id " +
                    "inner join jc_passport_office po_h on po_h.p_office_id = so.h_passport_office_id " +
                    "inner join jc_passport_office po_w on po_w.p_office_id = so.w_passport_office_id " +
                    "where student_order_status = ? order by student_order_date limit ?";

    private static final String SELECT_CHILD =
            "select soc.*, ro.r_office_area_id, ro.r_office_name " +
                    "from jc_student_child soc " +
                    "inner join jc_register_office ro on ro.r_office_id = soc.c_register_office_id " +
                    "where soc.student_order_id in ";

    private static final String SELECT_ORDERS_FULL =
            "select so.*, ro.r_office_area_id, ro.r_office_name, " +
                    "po_h.p_office_area_id as h_p_office_area_id, po_h.p_office_name as h_p_office_name, " +
                    "po_w.p_office_area_id as w_p_office_area_id, po_w.p_office_name as w_p_office_name, " +
                    "soc.*, ro_c.r_office_area_id, ro_c.r_office_name " +
                    "from jc_student_order so " +
                    "inner join jc_register_office ro on ro.r_office_id = so.register_office_id " +
                    "inner join jc_passport_office po_h on po_h.p_office_id = so.h_passport_office_id " +
                    "inner join jc_passport_office po_w on po_w.p_office_id = so.w_passport_office_id " +
                    "inner join jc_student_child soc on soc.student_order_id = so.student_order_id " +
                    "inner join jc_register_office ro_c on ro_c.r_office_id = soc.c_register_office_id " +
                    "where student_order_status = ? order by so.student_order_id limit ?";

    private Connection getConnection() throws SQLException {
        return ConnectionBuilder.getConnection();
    }

    @Override
    public Long saveStudentOrder(StudentOrder so) throws DaoException {
        Long result = -1L;
        logger.debug("SO: {}", so);
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(INSERT_ORDER, new String[]{"student_order_id"})) {
            con.setAutoCommit(false);
            try {
                // Header
                st.setInt(1, StudentOrderStatus.START.ordinal());
                st.setTimestamp(2, java.sql.Timestamp.valueOf(LocalDateTime.now()));
                // Husband and wife
                setParamsForAdult(st, 3, so.getHusband());
                setParamsForAdult(st, 18, so.getWife());
                // Marriage
                st.setString(33, so.getMarriageCertificateId());
                st.setLong(34, so.getMarriageOffice().getOfficeId());
                st.setDate(35, java.sql.Date.valueOf(so.getMarriageDate()));
                st.executeUpdate();
                ResultSet gkRs = st.getGeneratedKeys();
                if (gkRs.next()) {
                    result = gkRs.getLong(1);
                }
                saveChildren(con, so, result);
                con.commit();
            } catch (SQLException ex) {
                con.rollback();
                throw ex;
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }
        return result;
    }

    private void saveChildren(Connection con, StudentOrder so, Long soId) throws SQLException {
        try (PreparedStatement st = con.prepareStatement(INSERT_CHILD)) {
            for (Child child : so.getChildren()) {
                st.setLong(1, soId);
                setParamsForChild(st, child);
                st.addBatch();
            }
            st.executeBatch();
        }
    }

    private void setParamsForAdult(PreparedStatement st, int start, Adult adult) throws SQLException {
        setParamForPerson(st, start, adult);
        st.setString(start + 4, adult.getPassportSeria());
        st.setString(start + 5, adult.getPassportNumber());
        st.setDate(start + 6, java.sql.Date.valueOf(adult.getIssueDate()));
        st.setLong(start + 7, adult.getIssueDepartment().getOfficeId());
        setParamsForAddress(st, start + 8, adult);
        st.setLong(start + 13, adult.getUniversity().getUniversityId());
        st.setString(start + 14, adult.getStudentId());
    }

    private void setParamsForChild(PreparedStatement st, Child child) throws SQLException {
        setParamForPerson(st, 2, child);
        st.setString(6, child.getCertificateNumber());
        st.setDate(7, java.sql.Date.valueOf(child.getIssueDate()));
        st.setLong(8, child.getIssueDepartment().getOfficeId());
        setParamsForAddress(st, 9, child);
    }

    private void setParamForPerson(PreparedStatement st, int start, Person person) throws SQLException {
        st.setString(start, person.getSurName());
        st.setString(start + 1, person.getGivenName());
        st.setString(start + 2, person.getPatronymic());
        st.setDate(start + 3, Date.valueOf(person.getDayOfBirth()));
    }

    private void setParamsForAddress(PreparedStatement st, int start, Person person) throws SQLException {
        Address hAddress = person.getAddress();
        st.setString(start, hAddress.getPostCode());
        st.setLong(start + 1, hAddress.getStreet().getStreetCode());
        st.setString(start + 2, hAddress.getBuilding());
        st.setString(start + 3, hAddress.getExtension());
        st.setString(start + 4, hAddress.getApartment());
    }

    @Override
    public List<StudentOrder> getStudentOrders() throws DaoException {
        return getStudentOrdersOneSelect();
//        return getStudentOrdersTwoSelect();
    }

    private List<StudentOrder> getStudentOrdersOneSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(SELECT_ORDERS_FULL)) {
            Map<Long, StudentOrder> maps = new HashMap<>();
            st.setInt(1, StudentOrderStatus.START.ordinal());
            int limit = Integer.parseInt(Config.getProperty(Config.DB_LIMIT));
            st.setInt(2, limit);
            ResultSet rs = st.executeQuery();
            int counter = 0;
            while (rs.next()) {
                Long soId = rs.getLong("student_order_id");
                if (!maps.containsKey(soId)) {
                    StudentOrder so = getFullStudentOrder(rs);
                    result.add(so);
                    maps.put(soId, so);
                }
                StudentOrder so = maps.get(soId);
                so.addChild(fillChild(rs));
                counter++;
            }
            if (counter >= limit) {
                result.remove(result.size() - 1);
            }
            rs.close();
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }

        return result;
    }

    private List<StudentOrder> getStudentOrdersTwoSelect() throws DaoException {
        List<StudentOrder> result = new LinkedList<>();
        try (Connection con = getConnection();
             PreparedStatement st = con.prepareStatement(SELECT_ORDERS)) {
            st.setInt(1, StudentOrderStatus.START.ordinal());
            st.setInt(2, Integer.parseInt(Config.getProperty(Config.DB_LIMIT)));
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                StudentOrder so = getFullStudentOrder(rs);
                result.add(so);
            }
            findChildren(con, result);
            rs.close();
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new DaoException(ex);
        }

        return result;
    }

    private StudentOrder getFullStudentOrder(ResultSet rs) throws SQLException {
        StudentOrder so = new StudentOrder();
        fillStudentOrder(rs, so);
        fillMariage(rs, so);
        so.setHusband(fillAdult(rs, "h_"));
        so.setWife(fillAdult(rs, "w_"));
        return so;
    }

    private void fillStudentOrder(ResultSet rs, StudentOrder so) throws SQLException {
        so.setStudentOrderId(rs.getLong("student_order_id"));
        so.setStudentOrderDate(rs.getTimestamp("student_order_date").toLocalDateTime());
        so.setStudentOrderStatus(StudentOrderStatus.fromValue(rs.getInt("student_order_status")));
    }

    private void fillMariage(ResultSet rs, StudentOrder so) throws SQLException {
        so.setMarriageCertificateId(rs.getString("certificate_id"));
        so.setMarriageDate(rs.getDate("marriage_date").toLocalDate());

        Long roId = rs.getLong("register_office_id");
        String areaId = rs.getString("r_office_area_id");
        String name = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId, areaId, name);
        so.setMarriageOffice(ro);
    }

    private Adult fillAdult(ResultSet rs, String pref) throws SQLException {
        Adult adult = new Adult();
        adult.setSurName(rs.getString(pref + "sur_name"));
        adult.setGivenName(rs.getString(pref + "given_name"));
        adult.setPatronymic(rs.getString(pref + "patronymic"));
        adult.setDayOfBirth(rs.getDate(pref + "date_of_birth").toLocalDate());
        adult.setPassportSeria(rs.getString(pref + "passport_seria"));
        adult.setPassportNumber(rs.getString(pref + "passport_number"));
        adult.setIssueDate(rs.getDate(pref + "passport_date").toLocalDate());
        Long poId = rs.getLong(pref + "passport_office_id");
        String poArea = rs.getString(pref + "p_office_area_id");
        String poName = rs.getString(pref + "p_office_name");
        PassportOffice po = new PassportOffice(poId, poArea, poName);
        adult.setIssueDepartment(po);
        Address adr = new Address();
        Street street = new Street(rs.getLong(pref + "street_code"), "");
        adr.setStreet(street);
        adr.setPostCode(rs.getString(pref + "post_index"));
        adr.setBuilding(rs.getString(pref + "building"));
        adr.setExtension(rs.getString(pref + "extension"));
        adr.setApartment(rs.getString(pref + "appartment"));
        adult.setAddress(adr);
        University uni = new University(rs.getLong(pref + "university_id"), "");
        adult.setUniversity(uni);
        adult.setStudentId(rs.getString(pref + "student_number"));
        return adult;
    }

    private void findChildren(Connection con, List<StudentOrder> result) throws SQLException {
        String cl = "(" + result.stream().map(so -> String.valueOf(so.getStudentOrderId()))
                .collect(Collectors.joining(",")) + ")";

        Map<Long, StudentOrder> maps = result.stream().collect(Collectors
                .toMap(so -> so.getStudentOrderId(), so -> so));
        try (PreparedStatement st = con.prepareStatement(SELECT_CHILD + cl)) {
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                Child ch = fillChild(rs);
                StudentOrder so = maps.get(rs.getLong("student_order_id"));
                so.addChild(ch);
            }
        }
    }

    private Child fillChild(ResultSet rs) throws SQLException {
        String surName = rs.getString("c_sur_name");
        String givenName = rs.getString("c_given_name");
        String patronymic = rs.getString("c_patronymic");
        LocalDate dateOfBirth = rs.getDate("c_date_of_birth").toLocalDate();

        Child child = new Child(surName, givenName, patronymic, dateOfBirth);

        child.setCertificateNumber(rs.getString("c_certificate_number"));
        child.setIssueDate(rs.getDate("c_certificate_date").toLocalDate());

        Long roId = rs.getLong("c_register_office_id");
        String roArea = rs.getString("r_office_area_id");
        String roName = rs.getString("r_office_name");
        RegisterOffice ro = new RegisterOffice(roId, roArea, roName);
        child.setIssueDepartment(ro);

        Address adr = new Address();
        Street street = new Street(rs.getLong("c_street_code"), "");
        adr.setStreet(street);
        adr.setPostCode(rs.getString("c_post_index"));
        adr.setBuilding(rs.getString("c_building"));
        adr.setExtension(rs.getString("c_extension"));
        adr.setApartment(rs.getString("c_appartment"));
        child.setAddress(adr);
        return child;
    }
}
