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

# load environment variables
load_dotenv()

# check documentation for valid url patterns
db_key = os.getenv("DB_KEY")

if db_key == "" or db_key == None:
    raise Exception("No `DB_KEY` defined in environment variables.")

# db connection
db = psycopg.connect(db_key, cursor_factory=psycopg.ClientCursor)

# treats values of price
def treat_price(val):
    # split string price bh ` - `
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

# retrieve commodities from DB
def retrieve_commodities() -> pd.DataFrame:
    # retrieve from sql and format to dataframe
    commodities = pd.read_sql("SELECT * FROM commodity", con = db)

    return commodities

# retrieves markets from DB
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
    download_path = "/home/tope/Documents/Programming Files (v2)/Projects/crop-price-etl/temp"

    #
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path
    })

    # initialize session (use Chrome)
    driver = webdriver.Chrome(options=options)

    # navigate to source page
    driver.get(source)

    # retrieve firstmost report that contains daily reports
    report = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[5]/div/div/div[1]/article/div[2]/table/tbody/tr[1]/td[1]/a")

    # add download attribute
    driver.execute_script(f"arguments[0].setAttribute('download', '{report.text}');", report)

    # click button/link and redirect to PDF download
    report.click()

    # sleep temporarily
    time.sleep(2)

    return report.text

# function that extracts prices from DA file as a pandas DataFrame
def extract_prices(filename: str) -> pd.DataFrame:

    with pdfplumber.open(filename) as pdf:
        if (len(pdf.pages) == 2):
            tables = pdf.pages[1].extract_table()

            print(tables)

            market_prices = pd.DataFrame(tables)

            # ==== TRANSFORM =====
            market_prices = market_prices.iloc[range(2,market_prices.shape[0]),range(market_prices.shape[1])]
            # iterate through columns
            for i in range(market_prices.shape[1]):
                if i == 0:
                    # skip name
                    continue

                # treat numerical values
                market_prices[i] = market_prices[i].apply(treat_price)

            return market_prices
    return None

# function that loads prices to DB
def load_prices(market_prices: pd.DataFrame, filename:str) -> None:
    markets = retrieve_markets()
    commodities = retrieve_commodities()
    prices = []

    print(market_prices)

    # iterate per row
    for i, row in market_prices.iterrows():
        # check existence of market in db
        market_id = None

        if (markets[markets["name"] == row[0]].shape[0] == 0):
            # none found; insert to db (implement later)
            print(f"None found at row {i}. Skipping...", flush=True)
            continue
        else:
            market_id = i - 1

        # DEV NOTES
        # It would be much more efficient network-wise
        # if the market and commodity table is loaded into a dataframe
        # for the whole duration of the extraction.
        # iterate through each commodity

        j = 0
        for price in row:
            # skip title
            if j == 0:
                j += 1
                continue

            # insertion depends on length of price list
            if len(price) == 1:
                prices.append(
                    (
                        j+1,market_id,price[0],None,None,True,treat_date(filename)
                    )
                )
            elif len(price) == 2:
                prices.append(
                    (
                        j+1,market_id,None,price[0],price[1],True,treat_date(filename)
                    )
                )

            j += 1

    # initiate cursor object
    cur = db.cursor()

    # load prices to DB
    with cur.copy("COPY price (commodity_id, market_id,mean_price,minimum_price,maximum_price,is_available,date) FROM STDIN") as copy:
        for price in prices:
            print(price)
            copy.write_row(price)

    db.commit()

    return None

# function that extracts latest file
def extract_latest() -> bool:
    # retrieve latest
    filename = retrieve_latest_file()

    market_prices = extract_prices(f"./temp/{filename}.pdf")

    if type(market_prices) == pd.DataFrame:
        # load to db
        load_prices(market_prices, filename)
        print("Successful extraction.", flush=True)
    else:
        print("Unsuccessful extraction.")
        return False

    # delete file
    os.remove(f"./temp/{filename}.pdf")

    db.close()

    return True

# function that extracts the latest file from today
def extract_today() -> bool:
    # retrieve latest
    filename = retrieve_latest_file()

    if filename == None:
        return False

    # check date (must match today's date') temporarily commented
    if validate_date(filename.text):
        # short-circuit
        return False

    market_prices = extract_prices(f"./temp/{filename}.pdf")

    if type(market_prices) == pd.DataFrame:
        # load to db
        load_prices(market_prices, filename)
        print("Successful extraction.", flush=True)
    else:
        print("Unsuccessful extraction.", flush=True)
        return False

    db.close()

    return True

# function that attempts to extract today's prices (3 times)
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
        except:
            if (i < 2):
                print(f"Extraction attempt #{i+1} unsuccessful. Trying again.", flush=True)
            else:
                print(f"Extraction attempt #{i+1} unsuccessful. Exiting...", flush=True)
            continue

# function that extracts all files
# TODO: implement multi-threadding
def extract_all():
    # define source link
    source = "https://www.da.gov.ph/price-monitoring/"
    download_path = "/home/tope/Documents/Programming Files (v2)/Projects/crop-price-etl/temp"

    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": download_path
    })
    options.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])

    # initialize session (use Chrome)
    driver = webdriver.Chrome(options=options)

    # navigate to source page
    driver.get(source)

    # wait for proper load
    time.sleep(5)

    # hide popup
    popup = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[10]/div[2]/div/div/div/button")
    popup.click()

    # wait till popup is hidden
    time.sleep(5)

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
            market_prices = extract_prices(f"./temp/{filename}.pdf")

            if type(market_prices) == pd.DataFrame:
                # load to db
                load_prices(market_prices, filename)
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

    # for report in range(total):
        # print(reports[i].text, flush=True)

        # time.sleep(1)

        # if reports[i].text == "":
        #     break



    #     WebDriverWait(driver, 5)

    # print(reports, flush=True)
    # first 20
    #
    # print(reports[500], flush=True)
    # # f = r.find_element(by=By.TAG_NAME, value=f"a")

    # WebDriverWait(driver, 2)
    # print(file.text, flush=True)
    # for r in reports:
    #     print(r.text)
    #     filenames.append(r.text)
    #     i+=1


    # print(filenames, flush=True)
    print("End of extraction", flush=True)
    # add download attribute
    # driver.execute_script(f"arguments[0].setAttribute('download', '{report.text}');", report)

    # click button/link and redirect to PDF download
    # report.click()

    # # sleep temporarily
    # time.sleep(2)

    # return report.text
    return

run()
