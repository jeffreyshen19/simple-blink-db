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


def generate_test_small_skewed_dataset():
    '''
    [SCHEMA]
    id (int), quantity (int)
    '''
    # data is just id from 0-7 with the quantity column being 1,1,1,1,1,2,3
    with open('test_small_skewed_dataset.txt', 'w') as f:
        for i in range(5):
            f.write(str(i) + "," + str(1))
            f.write('\n')

        f.write(str(5) + "," + str(2))
        f.write('\n')

        f.write(str(6) + "," + str(3))
        f.write('\n')       

# generate_uniform_dataset(5000000) # 5 million rows
generate_test_small_skewed_dataset()
