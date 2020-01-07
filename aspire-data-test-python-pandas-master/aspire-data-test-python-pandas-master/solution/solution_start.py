import argparse
import glob
import datetime as dt
import pandas as pd
import pandas.io.parsers

def read_csv(csv_location: str):
    return pandas.read_csv(csv_location, header=0)

def read_json_folder(json_folder: str):
    transactions_files = glob.glob("{}*/*.json".format(json_folder))
    return pandas.concat(pandas.read_json(tf, lines=True) for tf in transactions_files)

def run_transformations(customers_location: str, products_location: str,
                        transactions_location: str, output_location: str):
    customers_df = read_csv(customers_location)
    products_df = read_csv(products_location)
    transactions_df = read_json_folder(transactions_location)
    selected_trans_df = get_latest_transaction_date(transactions_df)

    combine_df(customers_df,products_df,selected_trans_df,output_location)

    #return get_latest_transaction_date(transactions_df)

def combine_df(customers_df,products_df,selected_trans_df,output_location):
    #Merge betweed costomer and transaction dataframe
    com_df1 = pd.merge(selected_trans_df, customers_df, on = 'customer_id', how='left')

    rows = []
    for i, row in selected_trans_df.iterrows():
        row3 = {}
        cust_id = row['customer_id']
        basket  = row['basket']
        for row1 in basket:
            row3['customer_id'] = cust_id
            row3['product_id']  = row1['product_id']
            rows.append(row3)

    com_df2 = pd.DataFrame(rows)
    #Merge between product and transaction
    com_df3 = pd.merge(com_df2, products_df, on = 'product_id', how='left')
    com_df4 = pd.merge(com_df1, com_df3, on = 'customer_id', how='left')
    com_df4['purchase_count'] = 0
    com_df4['purchase_count'] = com_df4.groupby(['customer_id', 'product_id']).transform('count')
    com_df5 = com_df4.drop_duplicates(['customer_id', 'product_id'], keep='first')
    com_df5 = com_df5[['customer_id','loyalty_score','product_id','product_category','purchase_count']].sort_values(['customer_id','product_id'])
    outputfile = output_location + 'outfile' + str(dt.datetime.now().isoformat())[:10] + '.csv'

    # Creating outbound csv file based on com_df4
    com_df5.to_csv(outputfile, index=None, header=True)

def get_latest_transaction_date(transactions):
    latest_purchase = transactions.date_of_purchase.max()
    #print(latest_purchase)
    earliest_purchase = (dt.datetime.strptime(latest_purchase[:19], '%Y-%m-%d %H:%M:%S') - dt.timedelta(days = 8)).isoformat()
    #print(type(earliest_purchase))
    #latest_transaction = transactions[transactions.date_of_purchase == latest_purchase]
    filter1 = 'transactions.date_of_purchase > earliest_purchase'
    filter2 = 'transactions.date_of_purchase <= latest_purchase'
    #latest_transaction = transactions[filter1 & filter2]
    latest_transaction1 = transactions[transactions.date_of_purchase > earliest_purchase]
    latest_transaction = latest_transaction1[latest_transaction1.date_of_purchase <= latest_purchase]
    print(latest_transaction)
    return latest_transaction

def to_canonical_date_str(date_to_transform):
     return date_to_transform.strftime('%Y-%m-%d')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DataTest')

    parser.add_argument('--customers_location', required=False, default="C:\\SAINSBURYS\\gitrepo1\\aspire-data-test-python-master\\input_data\\starter\\customers.csv")
    parser.add_argument('--products_location', required=False, default="C:\\SAINSBURYS\\gitrepo1\\aspire-data-test-python-master\\input_data\\starter\\products.csv")
    parser.add_argument('--transactions_location', required=False, default="C:\\SAINSBURYS\\gitrepo1\\aspire-data-test-python-master\\input_data\\starter\\transactions\\")
    parser.add_argument('--output_location', required=False, default="C:\\SAINSBURYS\\gitrepo1\\aspire-data-test-python-master\\output_data\\outputs\\")

    args = vars(parser.parse_args())

    run_transformations(args['customers_location'], args['products_location'],
                        args['transactions_location'], args['output_location'])
