import pandas as pd 
import utils.postgres as postgres


from pathlib import Path
import sys
path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

def file_reader():
    curr_data = pd.read_csv('input_data/savingsplanrates.csv')
    print(type(curr_data))

if __name__ == '__main__':
    postgres.create_tables()
    postgres.load_data()