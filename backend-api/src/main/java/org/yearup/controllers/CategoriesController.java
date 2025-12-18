package org.yearup.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.yearup.data.CategoryDao;
import org.yearup.data.ProductDao;
import org.yearup.models.Category;
import org.yearup.models.Product;

import java.util.List;

// add the annotations to make this a REST controller
@RestController

// add annotation to allow cross site origin requests
@CrossOrigin("origins = http://localhost:8080")

public class CategoriesController
{
    private CategoryDao categoryDao;
    private ProductDao productDao;

    // create an Autowired controller to inject the categoryDao and ProductDao
    // CHECK HERE
    @Autowired
    public CategoriesController(CategoryDao categoryDao, ProductDao productDao){
        this.categoryDao = categoryDao;
        this.productDao = productDao;
    }


    // add the appropriate annotation for a get action

    @GetMapping("/categories")
    public List<Category> getAll()
    {
        // find and return all categories
        return categoryDao.getAllCategories();
    }

    // add the appropriate annotation for a get action
    @GetMapping("/categories/{id}")
    public Category getById(@PathVariable int id)
    {
        // get the category by id
        try {
            return categoryDao.getById(id);
        } catch (RuntimeException e){
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
    }

    // the url to return all products in category 1 would look like this
    // https://localhost:8080/categories/1/products
    @GetMapping("/{category_id}/products")
    public List<Product> getProductsById(@PathVariable int categoryId)
    {
        // get a list of product by categoryId
        return productDao.listByCategoryId(categoryId);
    }

    // add annotation to call this method for a POST action
    @PostMapping("/categories")
    // add annotation to ensure that only an ADMIN can call this function
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ResponseStatus(HttpStatus.CREATED)
    public Category addCategory(@RequestBody Category category)
    {
        // insert the category- MAKE SURE TO RETURN CATEGORY IN CREATE CATEGORY IN DAOCLASS
        return categoryDao.create(category);
    }

    // add annotation to call this method for a PUT (update) action - the url path must include the categoryId
    @PutMapping("/categories/{id}")
    // add annotation to ensure that only an ADMIN can call this function
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public void updateCategory(@PathVariable int id, @RequestBody Category category)
    {
        // update the category by id- CHECK AND UPDATE
        category.setCategoryId(id);
        categoryDao.update(id, category);
    }


    // add annotation to call this method for a DELETE action - the url path must include the categoryId
    @DeleteMapping("/categories/{id}")
    // add annotation to ensure that only an ADMIN can call this function
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    // If succesful reply with this status code
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCategory(@PathVariable int id)
    {
        // delete the category by id- CHECK AND UPDATE

        categoryDao.delete(id);
    }
}
