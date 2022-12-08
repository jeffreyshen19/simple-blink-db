import csv
import random

def generate_uniform_dataset(n):
    '''
    [SCHEMA]
    id (int), quantity (int)

    TODO: add price when we support floats
    '''

    # Get product names from https://data.world/datafiniti/consumer-reviews-of-amazon-products
    with open("product_names.txt", "r") as f:
        product_names = f.readlines()

    with open('test_uniform_dataset_' + str(n) + '.txt', 'w') as f:
        for id in range(n):
            product_name = product_names[random.randrange(0, len(product_names))].replace("\n", "")
            quantity = random.randint(1, 100)

            f.write(str(id) + "," + str(quantity))
            f.write('\n')

generate_uniform_dataset(5000000) # 5 million rows
