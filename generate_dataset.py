import csv
import random
import numpy as np

def generate_dataset(n):
    '''
    [SCHEMA]
    id (int), quantity (int), year (int)
    ---
    id: pkey
    quantity: drawn from uniform distribution from 1-100
    year: skewed distribution
    '''

    with open('test_dataset_5M.txt', 'w') as f:
        for id in range(n):
            quantity = random.randint(1, 100)
            year = np.random.choice([2010, 2011, 2012, 2013, 2014, 2015], p=[0.25, 0.20, 0.05, 0.10, 0.3, 0.1])

            f.write(str(id) + "," + str(quantity) + "," + str(year))
            f.write('\n')

generate_dataset(5000000)
