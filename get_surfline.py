import requests
import pprint
import json
import time
import re
import csv

from database import SnowflakeConnection
from aws import get_secret

wave_base_url = "https://services.surfline.com/kbyg/spots/forecasts/wave"
tides_base_url = "https://services.surfline.com/kbyg/spots/forecasts/tides"
wind_base_url = "https://services.surfline.com/kbyg/spots/forecasts/wind"
base_url = "https://services.surfline.com/kbyg/spots/forecasts/"
pp = pprint.PrettyPrinter(indent=1)
logs_path = "getsurf/surf_logs/"
id_file = "spot_ids.csv"

''' Data from the base url is only every 3 hrs, does not get finer
    granularity. Looks like need to request from wind waves and tide urls
    to get hour by hour info'''
def print_options(options):
    print("Select your spot. Enter '0' if none of these are your spot:")
    i = 1
    for hit in options:
        bc = hit['_source']['breadCrumbs']
        bc.reverse()
        print("\t (" + str(i) + ") - " + hit['_source']['name'] + " @ " + ', '.join(bc))
        i += 1

def get_selection(num_opts):
    selection = int(input())
    while (selection < 0 or selection > num_opts):
        print("Selection invalid. Please enter the number of a spot above or '0'")
        selection = int(input())

    return selection

def search(kywd, size=10):
    r = requests.get(f"https://services.surfline.com/search/site?q={kywd}&querySize={size}&suggestionSize={size}")
    hits = r.json()[0]['hits']['hits']
    suggest = r.json()[0]['suggest']['spot-suggest'][0]['options']
    print_options(hits)
    spots = hits
    selection = get_selection(len(hits))
    
    if (selection == 0):
        print_options(suggest)
        selection = get_selection(len(suggest))
        spots = suggest

    spot_name = spots[selection - 1]['_source']['name']
    print("Selected " + spot_name + ". Saving ID")
    spot_id = spots[selection - 1]['_id']
    return (spot_name.replace(' ', '_').replace(',',''), spot_id)
    
def get_spot_id():
    query = input("Where did you surf today?\n")

#     with open(id_file, 'rt') as f:
#         reader = csv.reader(f, delimiter=',')
#         for row in reader:
#             if query.lower() == row[0].lower():
#                 return (row[0], row[1])

    name, _id = search(query)
#     with open(id_file, 'a') as f:
#         write_str = name + "," + _id + "\n"
#         f.write(write_str)

    return (name, _id)

''' Give this function the full request '''
def save_http_data(fname, data):
    with open(fname, 'w') as outfile:
        datastring = json.dumps(data.json())
        outfile.write(datastring)

def save_json_data(fname, data):
    with open(fname, 'w') as outfile:
        datastring = json.dumps(data)
        outfile.write(datastring)

def read_data(fname):
    with open(fname) as datafile:
        return json.load(datafile)

def get_data(spot_id):
    r_wave = requests.get(wave_base_url + f"?spotId={spot_id}&days=1&intervalHours=1")
    r_tides = requests.get(tides_base_url + f"?spotId={spot_id}&days=1&intervalHours=1")
    r_wind = requests.get(wind_base_url + f"?spotId={spot_id}&days=1&intervalHours=1")

    return (r_wave.json(), r_tides.json(), r_wind.json())


def print_test_data():
    spot_id = "5c008f5313603c0001df5318" #get_spot_id()
    # print(spot_id)
    # r_all = requests.get(base_url + f"?spotId={spot_id}&days=1&intervalHours=1")
    # r_wave = requests.get(wave_base_url + f"?spotId={spot_id}&days=1&intervalHours=1")
    wave_data = read_data('wave_req_data.json')
    base_data = read_data('req_all_data.json')


    # waves = r_wave.json()['data']['wave']
    # all_waves = r_all.json()['data']['forecasts']
    # tides = r_all.json()['data']['tides']

    waves = wave_data['data']['wave']
    all_waves = base_data['data']['forecasts']
    tides = base_data['data']['tides']

    print("\n--------Data from All req---------------")
    pp.pprint(all_waves[0])
    print("\n--------Data from wave req---------------")
    pp.pprint(waves[0])
    print("\n----------------Tides--------------------")
    # pp.pprint(tides)
    timestamp = waves[0]['timestamp']
    for tide in tides:
        if timestamp == tide['timestamp']:
            pp.pprint(tide)

def print_time(secs):
    print(time.strftime("%a, %d %b %Y %I:%M:%S %p", time.localtime(secs)))

def get_surf_time(question):
    tstr = input(question)
    tstr = tstr.strip().replace(' ', '')

    # Add minutes if not present
    if ':' not in tstr:
        tstr = re.split('[ap]', tstr)[0] + ":00" + re.split('\d', tstr)[-1]

    # Format time string so strptime can read it
    if 'a' in tstr.lower():
        tstr = re.split('a', tstr)[0] + "AM"
    elif 'p' in tstr.lower():
        tstr = re.split('p', tstr)[0] + "PM"
    else:
        print("Invalid time input, please specify AM or PM")
        return -1

    now = time.localtime(time.time())
    ts = time.strptime(tstr, "%I:%M%p")

    surf_time = time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 
                            ts.tm_hour, ts.tm_min, 0, now.tm_wday, 
                            now.tm_yday, now.tm_isdst))
    return surf_time




if __name__ == "__main__":
    
    snow_object = SnowflakeConnection(
            user='surfline_user',
            password=get_secret('surfline_db_password').get('surfline_db_password'),
            account='bka04153',
            warehouse='surfline_wh',
            database='surfline',
            schema='surfline_logs',
            role='surfline_role'
        )
    
    
    spot_name, spot_id = get_spot_id()
    waves, tides, wind = get_data(spot_id)
    time_start = get_surf_time("When did you start?\n")
    time_end = get_surf_time("When did you end?\n")
    board = input("What board did you ride?\n")
    description = input("How was it?\n")

    surf_log = {'waves':waves, 'tides':tides, 'wind':wind,
                'time_start':time_start, 'time_end':time_end,
                'board':board, 'description':description,
                'spot':spot_name}


#     fname = logs_path + spot_name + "_" + time.strftime("%m_%d_%Y_%I%p", time.localtime(surf_log['time_start'])) + ".json" 
#     print(fname)
#     save_json_data(fname, surf_log)
    
    print(surf_log)
    
    snow_object.load_snowflake_json(json_data = surf_log)
    
    