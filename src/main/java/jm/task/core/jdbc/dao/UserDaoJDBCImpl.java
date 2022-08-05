package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class UserDaoJDBCImpl implements UserDao {

    private static final Logger log = Logger.getLogger(String.valueOf(UserDaoJDBCImpl.class));
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (Connection connection = Util.getConnection()) {
            String sql = "CREATE TABLE IF NOT EXISTS `test`.`users` " +
                    "(id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                    "name VARCHAR(50) NOT NULL, " +
                    "lastName VARCHAR(50) NOT NULL, " +
                    "age TINYINT)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            log.info("Ok. data base creating...");
        } catch (SQLException e) {
            log.warning("Missing create data base failing..." + e.getMessage());
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection()) {
            String sql = "DROP TABLE IF EXISTS `test`.`users`";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            log.info("Ok. data base deleting...");
        } catch (SQLException e) {
            log.warning("Missing delete data base failing..." + e.getMessage());
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection()) {
            String sql = "INSERT INTO `test`.`users` (name, lastName, age) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User с именем " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            log.warning("Missing save user data base failing..." + e.getMessage());
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection()) {
            String sql = "DELETE FROM `test`.`users` WHERE id = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            log.info("Ok. User remove by id success...");
        } catch (SQLException e) {
            log.warning("Missing remove user by id data base failing..." + e.getMessage());
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = Util.getConnection()) {
            String sql = "SELECT * FROM `test`.`users`";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery(sql);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                userList.add(user);
            }
            log.info("Ok. all users...");
        } catch (SQLException e) {
            log.warning("Missing get all users failing..." + e.getMessage());
        }
        return userList;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection()) {
            String sql = "DELETE FROM `test`.`users`";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            log.info("Ok. data base cleaning...");
        } catch (SQLException e) {
            log.warning("Missing data base clean failing..." + e.getMessage());
        }
    }
}
