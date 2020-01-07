import unittest
import solution.solution_start as ss

class MyTestCase(unittest.TestCase):
    def cust_prod_consider(self):
        outcsv_df = read_csv("C:\\SAINSBURYS\\gitrepo1\\aspire-data-test-python-master\\output_data\\outputs\\outfile2019-11-19.csv")
        test_df1 = ss.df1.drop_duplicates(['customer_id', 'product_id'], keep='first')
        df1 = pd.merge(ss.com_df2[['customer_id', 'product_id']], outcsv_df[['customer_id', 'product_id']], on = ['customer_id', 'product_id'], how='outer')
        #self.assertEqual(True, False)
        self.assertEqual(len(outcsv_df), len(test_df1), "Should be equal")


if __name__ == '__main__':
    unittest.main()
