package ch.bzz.refproject.data;

import ch.bzz.refproject.model.Category;
import ch.bzz.refproject.model.Project;
import ch.bzz.refproject.util.Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * data access for project entity
 * <p>
 * M151: RefProject
 *
 * @author Antoine Cleuvenot
 * @version 1.0
 * @since 2020/01/20
 */

public class ProjectDao implements Dao<Project, String>{

    /**
     * reads all Projects in table "Project"
     * @retrun list of Project with Status=A
     */
    @Override
    public List<Project> getAll() {
        ResultSet resultSet;
        List<Project> projectList = new ArrayList<>();
        String sqlQuery =
                "SELECT c.title, p.title, p.startDate, p.endDate " +
                        "  FROM Project AS p JOIN Category AS c " +
                        " WHERE c.status='A'";
        try {
            resultSet = MySqlDB.sqlSelect(sqlQuery);
            while (resultSet.next()) {
                Project project = new Project();
                setValues(resultSet, project);
                projectList.add(project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {

            MySqlDB.sqlClose();
        }
        return projectList;

    }

    /**
     * reads a project from the table "Project" identified by the projectUUID
     * @param projectUUID the primary key
     * @return project object
     */
    @Override
    public Project getEntity(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        ResultSet resultSet;
        Project project = new Project();

        String sqlQuery =
                "SELECT c.title, p.title, p.startDate, p.endDate " +
                        "  FROM Project AS p JOIN Category AS c USING (catergoryUUID)" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            resultSet = prepStmt.executeQuery();
            if (resultSet.next()) {
                setValues(resultSet, project);
            }

        } catch (SQLException sqlEx) {

            sqlEx.printStackTrace();
            throw new RuntimeException();
        } finally {
            MySqlDB.sqlClose();
        }
        return project;

    }

    /**
     * saves a project in the table "Project"
     * @param project the project object
     * @return Result code
     */
    @Override
    public Result save(Project project) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "REPLACE Project" +
                        " SET projectUUID='" + project.getProjectUUID() + "'," +
                        " categoryUUID='" + project.getCategory().getCategoryUUID() + "'," +
                        " title='" + project.getTitle() + "'," +
                        " startDate='" + project.getStartDate() + "'," +
                        " endDate=" + project.getEndDate() + "," +
                        " status='" + project.getStatus() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows <= 2) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }

    /**
     * deletes a project in the table "Project" identified by the projectUUID
     * @param projectUUID the primary key
     * @return Result code
     */
    @Override
    public Result delete(String projectUUID) {
        Connection connection;
        PreparedStatement prepStmt;
        String sqlQuery =
                "DELETE FROM Project" +
                        " WHERE projectUUID='" + projectUUID.toString() + "'";
        try {
            connection = MySqlDB.getConnection();
            prepStmt = connection.prepareStatement(sqlQuery);
            int affectedRows = prepStmt.executeUpdate();
            if (affectedRows == 1) {
                return Result.SUCCESS;
            } else if (affectedRows == 0) {
                return Result.NOACTION;
            } else {
                return Result.ERROR;
            }
        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
            throw new RuntimeException();
        }

    }
    /**
     * sets the values of the attributes from the resultset
     *
     * @param resultSet the resultSet with an entity
     * @param project      a project object
     * @throws SQLException
     */
    private void setValues(ResultSet resultSet, Project project) throws SQLException {
        project.setProjectUUID(resultSet.getString("projectUUID"));
        project.setCategory(new Category());
        project.setTitle(resultSet.getString("title"));
        project.setStatus(resultSet.getString("status"));
        project.setStartDate(resultSet.getString("startDate"));
        project.setEndDate(resultSet.getString("endDate"));
    }
}
