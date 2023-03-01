import pandas as pd 
import utils.postgres as postgres
import utils.calc_date as calc_date


from pathlib import Path
import sys
path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

def file_reader():
    curr_data = pd.read_csv('input_data/savingsplanrates.csv')
    print(type(curr_data))

if __name__ == '__main__':
    # variables
    start_date = input('start_date in d/m/Y, like 1/13/2023 ')
    end_date = input('end_date in d/m/Y, like 1/15/2023 ')
    commit_hourly_amount = int(input('AWS commit ammount (integer) '))

    end_date_plus_one = calc_date.curr_date_plus_one(end_date)

    start_date_sql = calc_date.change_date_pattern(start_date,"%m/%d/%Y","%Y-%m-%d")
    end_date_sql = calc_date.change_date_pattern(end_date_plus_one,"%m/%d/%Y","%Y-%m-%d")

    # load and transform
    postgres.create_tables()
    postgres.load_data()

    # calc billing
    sum_extra_due,sum_no_commit_price = postgres.get_sum_extra_due(
        start_date=start_date_sql,
        end_date=end_date_sql,
        commit_hourly_amount=commit_hourly_amount
    )

    nb_hours = calc_date.get_nb_hours(start_date,end_date_plus_one)
    commit_price = nb_hours * commit_hourly_amount
    commit_total = commit_price + sum_extra_due

    delta_price = sum_no_commit_price - commit_total

    # Display results
    print("Total without commitment $",sum_no_commit_price)
    print("Total with commitment (commitment + remaining on-demand) $", commit_total)
    print("Total savings $", delta_price)