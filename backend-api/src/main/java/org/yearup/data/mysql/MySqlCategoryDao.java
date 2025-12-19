package org.yearup.data.mysql;

import org.apache.ibatis.jdbc.SQL;
import org.springframework.stereotype.Component;
import org.yearup.data.CategoryDao;
import org.yearup.models.Category;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class MySqlCategoryDao extends MySqlDaoBase implements CategoryDao
{
    public MySqlCategoryDao(DataSource dataSource)
    {
        super(dataSource);
    }

    @Override
    public List<Category> getAllCategories()
    {
        // get all categories
        String sql = "SELECT * FROM Categories;";

        ArrayList<Category> categories = new ArrayList<>();

        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)){

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()){
                Category c = new Category(resultSet.getInt("category_id"), resultSet.getString("name"), resultSet.getString("description"));
                categories.add(c);
            }

            return categories;
        } catch (SQLException e){
            System.err.println("Uh oh, error: " + e);
        }
        return null;
    }

    @Override
    public Category getById(int categoryId)
    {
        // get category by id
        String sql = "SELECT * FROM categories where category_id = ?;";

        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ) {
            preparedStatement.setInt(1, categoryId);

            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()){
                return mapRow(resultSet);
            }
            return null;


        } catch (SQLException e) {
            e.printStackTrace();
        throw new RuntimeException("Not found");
        }
    }


    @Override
    public Category create(Category category)
    {
        // create a new category
        String sql = "INSERT INTO Categories (name, description) VALUES (?, ?);";

        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);){

            preparedStatement.setString(1, category.getName());
            preparedStatement.setString(2, category.getDescription());

            int rowsInserted = preparedStatement.executeUpdate();

            if (rowsInserted != 1) {
                System.err.println("Incorrect number of rows created.");
            }

            ResultSet resultSet = preparedStatement.getGeneratedKeys();

            resultSet.next();
            int categoryID = resultSet.getInt(1);
            resultSet.close();

            return new Category(categoryID, category.getName(), category.getDescription());

        } catch (SQLException e) {
            System.err.println("Error!: " + e);
        }
        return null;
    }

    @Override
    public void update(int categoryId, Category category)
    {
        // update category
        String sql = """
                UPDATE Categories
                SET name = ?, description = ?
                WHERE category_id = ?;
                """;
        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);){

            preparedStatement.setString(1, category.getName());
            preparedStatement.setString(2, category.getDescription());
            preparedStatement.setInt(3,categoryId);

            int rowsUpdated = preparedStatement.executeUpdate();
            System.out.println("Rows Updated: " + rowsUpdated);

            if (rowsUpdated != 1) {
                System.err.println("Rows Created: " + rowsUpdated );
                throw new RuntimeException("Number of rows updated is incorrect.");
            }
        } catch (SQLException e){
            System.err.println("Error!: " + e);
        }
    }

    @Override
    public void delete(int categoryId)
    {
        // delete category
        String sql = """
                DELETE FROM Categories
                WHERE Category_id = ?;
                """;

        try(Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);){

            preparedStatement.setInt(1, categoryId);

            int rowsDeleted = preparedStatement.executeUpdate();

            if (rowsDeleted != 1){
                throw new RuntimeException("Number of rows deleted is incorrect.");
            }
        } catch (SQLException e){
            System.err.println("Error!: " + e);
        }
    }

    private Category mapRow(ResultSet row) throws SQLException
    {
        int categoryId = row.getInt("category_id");
        String name = row.getString("name");
        String description = row.getString("description");

        Category category = new Category()
        {{
            setCategoryId(categoryId);
            setName(name);
            setDescription(description);
        }};

        return category;
    }

}
