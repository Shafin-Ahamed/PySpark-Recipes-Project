import unittest
from common_functions import *
from common_schemas import *
from configs import *

class TestETLScript(unittest.TestCase):

    # Here, I set up the variables to run my tests
    def setUp(self):
        self.initial_test_df = extract_and_transform_data("input_files/test_data/recipes-005.json")
        self.expected__initial_data = [('bologne', "3 cups of water. 2 cups of blue beef", "https://www.linkedin.com/feed/", "https://cdn.britannica.com/83/78183-004-345353F4/Stack-books.jpg", "PT30M", "4", "2024-08-02", "PT5M", "This is a test case"),
                                    ('bread', "3 cups of water. 2 cups of blue beef", "https://www.linkedin.com/feed/", "https://cdn.britannica.com/83/78183-004-345353F4/Stack-books.jpg", "PT30M", "4", "2024-08-02", "PT5M", "This is a test case"),
                                    ('fire', "3 cups of water. 2 cups of blue beef", "https://www.linkedin.com/feed/", "https://cdn.britannica.com/83/78183-004-345353F4/Stack-books.jpg", "PT2M", "4", "2024-08-02", "PT5M", "This is a test case"),
                                    ('bread', "3 cups of water. 2 cups of blue beef", "https://www.linkedin.com/feed/", "https://cdn.britannica.com/83/78183-004-345353F4/Stack-books.jpg", "PT50M", "4", "2024-08-02", "PT5M", "This is a test case"),
                                    ('bread', "3 cups of water. 2 cups of blue beef", "https://www.linkedin.com/feed/", "https://cdn.britannica.com/83/78183-004-345353F4/Stack-books.jpg", "PT5M", "4", "2024-08-02", "PT5M", "This is a test case")]
        self.expected_initial_columns = ["name", "ingredients", "url", "image", "cookTime", "recipeYield", "datePublished", "prepTime", "description"]
        self.expected_initial_df = spark.createDataFrame(data = self.expected__initial_data, schema = self.expected_initial_columns)
        self.result_df = run_etl("input_files/test_data/recipes-005.json", "output_files/test_data_output/recipes-005-export.csv")
        expected_data = [('Easy', 8.5), ('Medium', 41.67)]
        expected_columns = ["Difficulty", "Average_Cooking_Time_Per_Difficulty"]
        self.expected_df = spark.createDataFrame(data = expected_data, schema = expected_columns)

    def test_inital_frames(self):
        initial_test_data = self.initial_test_df.collect()
        initial_expected_data = self.expected_initial_df.collect()
        return set(initial_test_data) == set(initial_expected_data)
    
    # Here, I am checking the names of the columns for expected vs. results
    def test_for_column_names(self):
        self.assertListEqual(self.result_df.schema.names, self.expected_df.schema.names)

    # Here, I am checking for frame equality between expected and results.
    def test_final_frames(self):
        sample_data = self.result_df.collect()
        expected_data = self.expected_df.collect()
        return set(sample_data) == set(expected_data)


if __name__== '__main__':
    unittest.main()

