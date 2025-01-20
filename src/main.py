import pandas as pd
import psycopg
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, date
import time
import os
import pdfplumber
from dotenv import load_dotenv
import threading
import re

# DEV NOTE
# only files past December 15, 2022 are parseable by the current version.
# Possible fix: create separate parser for earlier files with different format.

# load environment variables
load_dotenv()

# ======= Shared Variables ========
db_key = os.getenv("DB_KEY") # check documentation for valid url patterns
download_path = os.getcwd() + "/temp"
# print(download_path)
options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", {
    "download.default_directory": download_path
})
# options.add_argument("--user-data-dir=/home/tope/.var/app/com.google.Chrome/config/google-chrome")
# options.add_argument("--profile-directory=Default")
# options.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
source = "https://www.da.gov.ph/price-monitoring/"
# =================================

if db_key == "" or db_key == None:
    raise Exception("No `DB_KEY` defined in environment variables.")

# db connection
# db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

# treats values of price
def treat_price(val):
    # split string price bh ` - `
    if val == None:
        return []

    splitted_val = val.split("-")

    # validate cells lacking data
    if (splitted_val[0] == "NOT AVAILABLE"):
        return []

    for i in range(len(splitted_val)):
        try:
            # parse string numeric to float
            splitted_val[i] = float(splitted_val[i])
        except:
            # short-circuit when error in parsing is found
            splitted_val = []
            break

    # return array
    return splitted_val

# fix row errors after extraction from pdf
def treat_row(row:list):
    i = 0
    for i in range(len(row)):
        # iterate through each value of the row

        # skip first and None values
        if i == 0 or row[i] == None:
            i += 1
            continue
        
        # mark index for disregard when blank
        if row[i] == "":
            i += 1
            continue
        
        res = re.search("^[0-9][0-9]*.[0-9][0-9]$", row[i])

        # check if value is single number[
        if re.search("^[0-9][0-9]*.[0-9][0-9]$", row[i]) != None:
            # fix cell if succeeding is a dash (-)
            if i < len(row) - 2 and row[i+1] == "-":
                row[i] = f"{row[i]}-{row[i+2]}"
                # set skipped rows to blank
                row[i+1] = None
                row[i+2] = None

                i += 2 # skip 2 indices
        if (type(row[i]) == str):
            row[i] = row[i].replace(" ", "")

    return row

# retrieve commodities from DB
# DEPRECATED
def retrieve_commodities() -> pd.DataFrame:
    # retrieve from sql and format to dataframe
    commodities = pd.read_sql("SELECT * FROM commodity", con = db)

    return commodities

# retrieves markets from DB
# DEPRECATED
def retrieve_markets() -> pd.DataFrame:
    # retrieve from sql and format to dataframe
    commodities = pd.read_sql("SELECT * FROM market", con = db)

    return commodities

# date validator
def validate_date(report_name: str) -> bool:
    # split report name by space
    splitted_name = report_name.split(" ")

    # length must be 3
    if len(splitted_name) != 3:
        return False

    splitted_name[1] = splitted_name[1].split(",")[0] # keep only number

    # deal with month
    if splitted_name[0] == "January":
        splitted_name[0] = 1
    elif splitted_name[0] == "February":
        splitted_name[0] = 2
    elif splitted_name[0] == "March":
        splitted_name[0] = 3
    elif splitted_name[0] == "April":
        splitted_name[0] = 4
    elif splitted_name[0] == "May":
        splitted_name[0] = 5
    elif splitted_name[0] == "June":
        splitted_name[0] = 6
    elif splitted_name[0] == "July":
        splitted_name[0] = 7
    elif splitted_name[0] == "August":
        splitted_name[0] = 8
    elif splitted_name[0] == "September":
        splitted_name[0] = 9
    elif splitted_name[0] == "October":
        splitted_name[0] = 10
    elif splitted_name[0] == "November":
        splitted_name[0] = 11
    elif splitted_name[0] == "December":
        splitted_name[0] = 12
    else:  # Handle all other cases where splitted_name[0] is not a valid month
        return False

    try:
        # build datetime
        report_date = datetime.strptime(f'{splitted_name[0]}/{splitted_name[1]}/{splitted_name[2]}', "%M/%d/%Y").date()

        # compare datetimes
        if date.today() != report_date:
            return False

        return True
    except:
        return False

def treat_date(string_date:str) -> str|None:
    # split report name by space
    splitted_name = string_date.split(" ")

    # length must be 3
    if len(splitted_name) != 3:
        return False

    splitted_name[1] = splitted_name[1].split(",")[0] # keep only number

    # deal with month
    if splitted_name[0] == "January":
        splitted_name[0] = 1
    elif splitted_name[0] == "February":
        splitted_name[0] = 2
    elif splitted_name[0] == "March":
        splitted_name[0] = 3
    elif splitted_name[0] == "April":
        splitted_name[0] = 4
    elif splitted_name[0] == "May":
        splitted_name[0] = 5
    elif splitted_name[0] == "June":
        splitted_name[0] = 6
    elif splitted_name[0] == "July":
        splitted_name[0] = 7
    elif splitted_name[0] == "August":
        splitted_name[0] = 8
    elif splitted_name[0] == "September":
        splitted_name[0] = 9
    elif splitted_name[0] == "October":
        splitted_name[0] = 10
    elif splitted_name[0] == "November":
        splitted_name[0] = 11
    elif splitted_name[0] == "December":
        splitted_name[0] = 12
    else:  # Handle all other cases where splitted_name[0] is not a valid month
        return False

    try:
        # build datetime
        report_date = datetime.strptime(f'{splitted_name[0]}/{splitted_name[1]}/{splitted_name[2]}', "%M/%d/%Y").date()

        # compare datetimes

        return report_date.strftime("%Y-%m-%d")
    except:
        return None

# scrape DA site to find latest file
# returns: filename of latest
def retrieve_latest_file() -> str | None:
    # define source link
    source = "https://www.da.gov.ph/price-monitoring/"

    # initialize session (use Chrome)
    # driver = webdriver.Chrome(options=options)
    driver = webdriver.Chrome(options=options)

    # navigate to source page
    driver.get(source)

    # wait for site to load
    time.sleep(3)

    # get exit button of popup
    try:
        exit_popup_button = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[10]/div[2]/div/div/div/button")

        if (exit_popup_button != None):
            # click the button
            exit_popup_button.click()

            time.sleep(3)
    except:
        print()
    

    # retrieve firstmost report that contains daily reports
    report = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody/tr[1]/td[1]/a")

    # add download attribute
    driver.execute_script(f"arguments[0].setAttribute('download', '{report.text}');", report)

    # click button/link and redirect to PDF download
    report.click()

    # sleep temporarily
    time.sleep(2)

    report_name = report.text

    driver.close()

    return report_name

# function that extracts prices from DA file as a pandas DataFrame
def extract_prices(filename: str):

    with pdfplumber.open(filename) as pdf:
        if (len(pdf.pages) >= 2):
            # extract table
            table = pdf.pages[1].extract_table()

            i = 0
            for i in range(len(table)):
                # treat each row
                table[i] = treat_row(table[i])

            # parse list of rows to DataFrame
            market_prices = pd.DataFrame(table)

            columns = list(filter(lambda val: val != None, table[1]))
            # print(columns)

            # ==== TRANSFORM =====
            # remove empty header row
            market_prices = market_prices.iloc[range(2,market_prices.shape[0]),range(market_prices.shape[1])]

            # drop columns with `None` headers
            market_prices.dropna(axis=1, how="all", inplace=True)

            # reset indices
            market_prices.columns = range(market_prices.columns.size)   

            # iterate through columns
            for i in range(market_prices.shape[1]):
                if i == 0:
                    continue
                
                market_prices[i] = market_prices[i].apply(treat_price)

            market_prices.iloc[0] = columns

            return market_prices
    return None

# function that loads prices to DB
def load_prices(market_prices: pd.DataFrame, filename:str, db) -> None:
    commodities = market_prices.iloc[0]
    prices = []

    # iterate per row
    for i, row in market_prices.iterrows():
        j = 0
        market_name = None
        for price in row:
            ## skip title
            if j == 0:
                market_name = price
                j += 1
                continue
                
            if price == None:
                prices.append(
                    (
                        commodities[j],market_name,None,None,None,treat_date(filename)
                    )
                )
                j += 1
                continue

            # insertion depends on length of price list
            if len(price) == 1:
                prices.append(
                    (
                        commodities[j],market_name,price[0],None,None,treat_date(filename)
                    )
                )
            elif len(price) == 2:
                prices.append(
                    (
                        commodities[j],market_name,None,price[0],price[1],treat_date(filename)
                    )
                )

            j += 1

    # initiate cursor object
    cur = db.cursor()

    # load prices to DB
    with cur.copy("COPY initial_price (commodity_name, market_name,mean_price,minimum_price,maximum_price,date) FROM STDIN") as copy:
        for price in prices:
            copy.write_row(price)

    db.commit()

    return None

# function that extracts latest file
def extract_latest() -> bool:
    # retrieve latest
    filename = retrieve_latest_file()
    
    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    # check if file has been extracted
    commodities = pd.read_sql("SELECT * FROM retrieved_files", con = db)
    if commodities.empty == False:
        print(f"{filename}.pdf has been retrieved before.")
        return False
    
    # extract prices
    market_prices = extract_prices(f"./temp/{filename}.pdf")

    if type(market_prices) == pd.DataFrame:
        # load to db
        load_prices(market_prices, filename, db)
        print("Successful extraction.", flush=True)

        # load retrieved file as success
        db.execute("""
                    INSERT INTO retrieved_files (date)
                    VALUES (%s);
                   """, (treat_date(filename),))
        db.commit()
    else:
        print("Unsuccessful extraction.")

        # load retrieved file as failed
        db.execute("""
                    INSERT INTO retrieved_files (date, is_success)
                    VALUES (%s);
                   """, (treat_date(filename), False))
        db.commit()
        return False

    # delete file
    os.remove(f"{download_path}/{filename}.pdf")

    

    db.close()

    return True

# function that extracts the latest file from today
def extract_today() -> bool:
    # retrieve latest
    filename = retrieve_latest_file()

    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    if filename == None:
        return False

    # check date (must match today's date') temporarily commented
    if validate_date(filename.text):
        # short-circuit
        return False

    market_prices = extract_prices(f"./temp/{filename}.pdf")

    if type(market_prices) == pd.DataFrame:
        # load to db
        load_prices(market_prices, filename, db)
        print("Successful extraction.", flush=True)
    else:
        print("Unsuccessful extraction.", flush=True)
        return False

    db.close()

    return True

# function that attempts to extract latest prices (3 times)
def run():
    # try 3 times
    for i in range(3):
        try:
            result = extract_latest()
            if result == False:
                if (i < 2):
                    print(f"Extraction attempt #{i+1} unsuccessful. Trying again.", flush=True)
                else:
                    print(f"Extraction attempt #{i+1} unsuccessful. Exiting...", flush=True)
                continue
            else:
                return
        except Exception as e:
            if (i < 2):
                print(f"Extraction attempt #{i+1} unsuccessful. Trying again.", flush=True)
            else:
                print(f"Extraction attempt #{i+1} unsuccessful. Exiting...", flush=True)
            print(f"Error:\n{e}")
            continue

def load_file_record(filename, db, success=True) -> None:

    is_valid_filename = True

    try:
        if type(filename) != str:
            is_valid_filename = False
        else:
            treat_date(filename)
    except:
        is_valid_filename = False
                
    # load retrieved file along with its status
    db.execute("""
                INSERT INTO retrieved_files (date, is_success)
                VALUES (%s, %s);
                """, (treat_date(filename), success))            
    db.commit()


def extract_portions(start:int, end:int, db):
    # initialize new Chrome driver
    driver = webdriver.Chrome(options=options)

    # NOTE: CREATE OWN INSTANCE OF DB DRIVER TO AVOID MULTI-THREADING ISSUE

    # navigate to source page
    driver.get(source)

    # wait for proper load
    time.sleep(5)

    # hide popup
    # get exit button of popup
    try:
        exit_popup_button = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[10]/div[2]/div/div/div/button")

        if (exit_popup_button != None):
            # click the button
            exit_popup_button.click()

            time.sleep(3)
    except:
        print()

    filename = ""

    for i in range(end):
        try:
            # attempt to deal with every file here
            f = driver.find_element(by=By.XPATH, value=f"/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody/tr[{start+i}]/td[1]/a")

            # save filename
            filename = f.text

            # add download attribute
            driver.execute_script(f"arguments[0].setAttribute('download', '{f.text}');", f)

            # click button/link and redirect to PDF download
            f.click()

            # sleep temporarily
            time.sleep(2)

            # extract prices from saved file
            market_prices = extract_prices(f"./temp/{filename}.pdf") # fix

            if type(market_prices) == pd.DataFrame:
                # load to db
                load_prices(market_prices, filename, db)
                # log_file.write(f"Successful extraction of {filename}.pdf\n")
                print(f"Successful extraction of {filename}.pdf.", flush=True)

                # delete file
                os.remove(f"./temp/{filename}.pdf")

                # load retrieved file as success
                load_file_record(filename, db, True)
            else:
                # log_file.write(f"Unsuccessful extraction of {filename}.pdf\n")
                print(f"Unsuccessful extraction of {filename}.pdf.", flush=True)
                # load retrieved file as failed
                load_file_record(filename, db, False)
        except Exception as error:
            # skip row with error, continue extraction
            print(f"Unsuccessful extraction of {filename}.pdf", flush=True)
            print(f"Exception occured: {error}", flush=True)

            # load retrieved file as failed
            load_file_record(filename, db, False)
            continue

# function that extracts all files
def extract_all():
    # define source link
    # source = "https://www.da.gov.ph/price-monitoring/"
    # download_path = "/home/tope/Documents/Programming Files (v2)/Projects/crop-price-etl/temp"

    # initialize session (use Chrome)
    driver = webdriver.Chrome(options=options)

    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    # navigate to source page
    driver.get(source)

    # wait for proper load
    time.sleep(5)

    # hide popup
    popup = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[10]/div[2]/div/div/div/button")

    time.sleep(3)

    popup.click()

    # wait till popup is hidden
    time.sleep(3)

    # retrieve table containing files
    report_container = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody")

    # retrieve reports
    reports = report_container.find_elements(by=By.TAG_NAME, value="tr")

    # get total count of reports
    total = len(reports)
    print(f"Found a total of {total} elements", flush=True)

    log_file = open("log.txt", "w")

    filename = ""

    for i in range(total):
        try:
            # attempt to deal with every file here
            f = driver.find_element(by=By.XPATH, value=f"/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody/tr[{i+1}]/td[1]/a")

            # save filename
            filename = f.text

            # add download attribute
            driver.execute_script(f"arguments[0].setAttribute('download', '{f.text}');", f)

            # click button/link and redirect to PDF download
            f.click()

            # sleep temporarily
            time.sleep(2)

            # extract prices from saved file 
            market_prices = extract_prices(f"./temp/{filename}.pdf") # FIX

            if type(market_prices) == pd.DataFrame:
                # load to db
                load_prices(market_prices, filename, db)
                log_file.write(f"Successful extraction of {filename}.pdf\n")
                print(f"Successful extraction of {filename}.pdf.", flush=True)

                # delete file
                os.remove(f"./temp/{filename}.pdf")
            else:
                log_file.write(f"Unsuccessful extraction of {filename}.pdf\n")
                print(f"Unsuccessful extraction of {filename}.pdf.", flush=True)
                return False
        except Exception as error:
            # skip row with error, continue extraction
            log_file.write(f"Unsuccessful extraction of {filename}.pdf\n")
            print(f"Unsuccessful extraction of {filename}.pdf", flush=True)
            print(f"Exception occured: {error}", flush=True)
            continue

    print("End of extraction", flush=True)

    return

# multi-threadded version of extract_all
def extract_all_multithread():
    # last working file
    last_working_file_name = "December 15, 2022"

    # initialize session (use Chrome)
    driver = webdriver.Chrome(options=options)

    db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

    # navigate to source page
    driver.get(source)

    # wait for proper load
    time.sleep(5)

    # get exit button of popup then hide if existing
    try:
        exit_popup_button = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[10]/div[2]/div/div/div/button")

        if (exit_popup_button != None):
            # click the button
            exit_popup_button.click()

            time.sleep(3)
    except:
        print()

    # retrieve table containing files
    report_container = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody")

    # retrieve reports
    reports = report_container.find_elements(by=By.TAG_NAME, value="tr")

    # get total count of reports
    total = len(reports)

    # count how many till reaching guaranteed working file
    for i in range(total):
        row = driver.find_element(By.XPATH, value=f"/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody/tr[{i+1}]/td[1]/a")
        if row.text == last_working_file_name:
            # set new total
            total = i+1
            break

    print(f"Found a total of {total} elements", flush=True)

    log_file = open("log.txt", "w")

    filename = ""

    driver.close()

    # create threads
    t1 = threading.Thread(target=extract_portions, args=(1, int(total/2), db,))
    t2 = threading.Thread(target=extract_portions, args=(int(total/2)+1,total, db))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    return

extract_all_multithread()