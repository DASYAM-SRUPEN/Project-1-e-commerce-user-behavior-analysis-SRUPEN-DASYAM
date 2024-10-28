from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd

# Load product data (replace with your actual products.csv path)
products_df = pd.read_csv('input/products.csv')

class MRProductRevenue(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_revenue,
                   reducer=self.reducer_total_revenue),
            MRStep(reducer=self.reducer_collect_and_sort),
            MRStep(reducer=self.reducer_find_top3)
        ]
    
    # Mapper: Emit (product_id, revenue)
    def mapper_revenue(self, _, line):
        try:
            fields = line.split(',')
            if fields[0] != "TransactionID":  # Skip header row
                product_id = fields[3]
                revenue = float(fields[5])  # RevenueGenerated
                yield product_id, revenue
        except Exception as e:
            pass  # Skip any errors
    
    # Reducer 1: Aggregate the total revenue per product
    def reducer_total_revenue(self, product_id, revenues):
        total_revenue = sum(revenues)
        yield product_id, total_revenue
    
    # Reducer 2: Collect product revenues and associate them with categories
    def reducer_collect_and_sort(self, product_id, total_revenues):
        product_info = products_df.loc[products_df['ProductID'] == int(product_id)]
        if not product_info.empty:
            product_category = product_info['ProductCategory'].values[0]
            product_name = product_info['ProductName'].values[0]
            total_revenue = sum(total_revenues)
            
            # Emit as (category, (product_name, total_revenue))
            yield product_category, (product_name, total_revenue)
    
    # Reducer 3: Find and output the top 3 most profitable products in each category
    def reducer_find_top3(self, category, product_revenues):
        # Collect all products in this category, sort by revenue, and return top 3
        sorted_products = sorted(product_revenues, key=lambda x: x[1], reverse=True)
        top_3 = sorted_products[:3]
        
        for product in top_3:
            yield category, product

if __name__ == '__main__':
    MRProductRevenue.run()
