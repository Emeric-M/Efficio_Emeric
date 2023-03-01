from datetime import datetime, timedelta
from math import ceil

def curr_date_plus_one(input_date):
    pattern = "%m/%d/%Y"
    date_plus_one = datetime.strptime(input_date, pattern) + timedelta(days=1)
    return date_plus_one.strftime(pattern)

def change_date_pattern(input_date,src_pattern,dest_pattern):
    return (datetime.strptime(input_date, src_pattern)).strftime(dest_pattern)

def get_nb_hours(start_date,end_date):
    start_datetime = datetime.strptime(start_date , "%m/%d/%Y")
    end_datetime = datetime.strptime(end_date, "%m/%d/%Y")

    delta = end_datetime - start_datetime
    delta_hours = delta.days * 24

    return delta_hours