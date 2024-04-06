import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# print all of pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

logging.basicConfig(filename='results.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

max_rank = 100
max_open_trades = 4
max_daily_loss = -10
max_trade_loss = -15
gain_target = 1.07
highest_rank_limit = 100
cooldown_weeks = 1


def main():
    delete_old_log()
    logging.info('Program started')
    today_datetime = datetime.now()
    start_date = "10/14/2023"
    date_format = "%m/%d/%Y"  # Define the format of the date string
    start_datetime = datetime.strptime(start_date, date_format)  # Convert the string to a datetime object

    loop_datetime = start_datetime

    tracking_pd = create_tracking_panda()

    while (loop_datetime + timedelta(days=1)) < today_datetime:
        # Increment loop_datetime by one day for the next iteration
        loop_datetime += timedelta(days=1)
        reformatted_loop_datetime_str = loop_datetime.strftime("%Y-%m-%d")
        print(reformatted_loop_datetime_str)

        this_loop_file_location = get_daily_file_location(loop_datetime)
        this_loop_daily_panda = pd.read_excel(this_loop_file_location)

        tracking_pd = update_open_trades(tracking_pd, this_loop_daily_panda, reformatted_loop_datetime_str)
        tracking_pd = look_for_new_trades(this_loop_daily_panda, tracking_pd, reformatted_loop_datetime_str)

        print(tracking_pd.to_string(index=False))
        logging.info(f'{tracking_pd.to_string(index=False)}')

    dump_to_excel(tracking_pd)

    results = tracking_pd.to_string(index=False)
    print(results)
    logging.info('Program ended')


def delete_old_log():
    file_path = "results.log"

    # Open the file in write mode to clear its contents
    with open(file_path, "w") as file:
        pass  # No need to write anything, just open and close it

    print(f"Contents of {file_path} cleared.")


def update_open_trades(tracking_pd, daily_panda, reformatted_loop_datetime_str):
    today_date = datetime.now().date()
    updated_open_trades = tracking_pd.copy()

    for index, row in updated_open_trades.iterrows():
        coin = row['Coin']
        name = row['Name']

        if row['Status'] == "Open":
            daily_row = daily_panda[daily_panda['Name'] == name]
            cmc_price = daily_row['CMC_Price'].iloc[0]
            buy_price = row['BuyPrice']
            one_day = row['24-Hour']

            # update high price
            current_high_price = row['HighPrice']
            if current_high_price < cmc_price:
                updated_open_trades.loc[index, 'HighPrice'] = cmc_price

            # update current price
            current_price = cmc_price
            updated_open_trades.loc[index, 'CurrentPrice'] = current_price

            # update current gain
            current_gain = ((current_price - buy_price) / buy_price) * 100
            # Limit to 2 decimal places
            current_gain_formatted = "{:.2f}".format(current_gain)
            # Convert back to float
            current_gain_float = float(current_gain_formatted)
            updated_open_trades.loc[index, 'CurrentGain'] = current_gain_float

            updated_trade_date = pd.to_datetime(updated_open_trades['BuyDate'])
            loop_date = datetime.strptime(reformatted_loop_datetime_str, "%Y-%m-%d")
            cooldown_period = loop_date + timedelta(weeks=cooldown_weeks)
            formatted_cooldown_period = cooldown_period.strftime("%Y-%m-%d")

            if current_price > row['GainTarget']:
                print(coin, "has closed for profit!")
                # time.sleep(1)
                # Update status, sale price, sale date, and profit
                updated_open_trades.loc[index, 'Status'] = "Closed"
                updated_open_trades.loc[index, 'SalePrice'] = buy_price * gain_target
                updated_open_trades.loc[index, 'SaleDate'] = reformatted_loop_datetime_str
                updated_open_trades.loc[index, 'Profit'] = gain_target
                updated_open_trades.loc[index, 'CoolDownUntil'] = formatted_cooldown_period
                logging.info(f'Closing trade for {coin}')
                updated_open_trades.loc[index, 'SaleReason'] = "Profit"

            elif one_day < max_daily_loss:
                print(coin, "has closed at max daily loss!")
                # time.sleep(1)
                logging.info(f'Closing trade for max daily {coin}')
                formatted_cooldown_period = cooldown_period.strftime("%Y-%m-%d")
                # Update status, sale price, sale date, and profit
                updated_open_trades.loc[index, 'Status'] = "Closed"
                updated_open_trades.loc[index, 'SalePrice'] = current_price
                updated_open_trades.loc[index, 'SaleDate'] = reformatted_loop_datetime_str
                updated_open_trades.loc[index, 'Profit'] = current_gain_formatted
                updated_open_trades.loc[index, 'CoolDownUntil'] = formatted_cooldown_period
                updated_open_trades.loc[index, 'SaleReason'] = "Max daily loss met"

            elif current_gain < max_trade_loss:
                print(coin, "has closed at max total loss!")
                # time.sleep(1)
                logging.info(f'Closing trade for max total loss {coin}')
                formatted_cooldown_period = cooldown_period.strftime("%Y-%m-%d")
                # Update status, sale price, sale date, and profit
                updated_open_trades.loc[index, 'Status'] = "Closed"
                updated_open_trades.loc[index, 'SalePrice'] = buy_price * max_trade_loss
                updated_open_trades.loc[index, 'SaleDate'] = reformatted_loop_datetime_str
                updated_open_trades.loc[index, 'Profit'] = -15
                updated_open_trades.loc[index, 'CoolDownUntil'] = formatted_cooldown_period
                updated_open_trades.loc[index, 'SaleReason'] = "Max total loss met"

    return updated_open_trades


def look_for_new_trades(daily_panda, tracking_pd, reformatted_loop_datetime_str):
    results = "\n" + reformatted_loop_datetime_str

    # limit to coinbase and kraken
    daily_panda = scope_exchanges(daily_panda)
    daily_panda = apply_rank_cutoff(daily_panda)

    daily_panda = daily_panda.sort_values(by='90-Day', ascending=False)
    daily_panda = daily_panda.head(max_open_trades)

    for _, daily_panda_row in daily_panda.iterrows():
        coin = daily_panda_row['Coin']
        name = daily_panda_row['Name']

        open_trades_count = len(tracking_pd[tracking_pd['Status'] == 'Open'])
        if open_trades_count <= max_open_trades:
            print("has available trades, count is", open_trades_count)
            logging.info(f'Open trades:{open_trades_count}')
            under_max_trades = True
        else:
            under_max_trades = False

        if under_max_trades:
            logging.info('has available trades so looking for new ones')
        else:
            print("at max open trades")
            logging.info(f'no room for more trades{coin}')

        has_open_trade = check_if_has_open_trade(tracking_pd, name)
        has_past_trades = check_if_has_past_trades(tracking_pd, name)

        if has_past_trades:  # check if still in cooldown mode
            last_trade_row = get_last_trade_row(tracking_pd, name)
            cool_down_date = last_trade_row['CoolDownUntil']
            cool_down_date = pd.to_datetime(cool_down_date).date()
            reformatted_loop_date = datetime.strptime(reformatted_loop_datetime_str, "%Y-%m-%d").date()

            if reformatted_loop_date > cool_down_date:
                cooldown_over = True
            else:
                cooldown_over = False

            if cooldown_over:
                print("cooldown mode done for", coin)
            else:
                print("still in cooldown mode for", coin)
        else:
            cooldown_over = True

        if cooldown_over and not has_open_trade and not under_max_trades:
            print("not enough room to open trade for", coin)
            logging.info(f'Not enough slots to buy {coin}')

        if cooldown_over and under_max_trades and not has_open_trade:
            print("opening new trade for", coin)
            # time.sleep(1)
            new_row = create_new_row(daily_panda_row, reformatted_loop_datetime_str)
            tracking_pd = pd.concat([tracking_pd, new_row], ignore_index=True, sort=False)
            logging.info(f'Buying {coin}')

    print(results)

    return tracking_pd


def create_new_row(row, reformatted_loop_datetime_str):
    gain_target_price = (row['CMC_Price'] * gain_target)

    new_row = {
        'Rank': row['CMC_Rank'],
        'Coin': row['Coin'],
        'Name': row['Name'],
        'Status': "Open",
        '24-Hour': round(row['24-Hour'], 0),  # Round to 0 decimal places
        '7-Day': round(row['7-Day'], 0),  # Round to 0 decimal places
        '30-Day': round(row['30-Day'], 0),  # Round to 0 decimal places
        '60-Day': round(row['60-Day'], 0),  # Round to 0 decimal places
        '90-Day': round(row['90-Day'], 0),  # Round to 0 decimal places
        'BuyPrice': row['CMC_Price'],
        'HighPrice': row['CMC_Price'],
        'BuyDate': reformatted_loop_datetime_str,
        'CurrentPrice': row['CMC_Price'],
        'CurrentGain': "0",
        'GainTarget': gain_target_price
    }

    return pd.DataFrame([new_row])  # Convert the dictionary to DataFrame


def get_last_trade_row(tracking_pd, name):
    # Filter rows by name
    name_rows = tracking_pd[tracking_pd['Name'] == name].copy()  # Ensure we're working with a copy

    # Check if there are any rows for the given name
    if name_rows.empty:
        return None  # If no rows found for the name, return None

    # Convert 'CoolDownUntil' column to datetime using .loc
    name_rows.loc[:, 'CoolDownUntil'] = pd.to_datetime(name_rows['CoolDownUntil'], errors='coerce')

    # Drop rows where 'CoolDownUntil' couldn't be converted to datetime (e.g., NaN values)
    name_rows.dropna(subset=['CoolDownUntil'], inplace=True)

    # Find the row with the most recent date within the cooldown date column
    last_trade_row = name_rows.loc[name_rows['CoolDownUntil'].idxmax()]

    return last_trade_row


def check_if_has_open_trade(open_trades_pd, name):
    # Filter the DataFrame based on the value in the "Name" column
    filtered_rows = open_trades_pd[open_trades_pd['Name'] == name]

    # Check if any rows match the given name
    if not filtered_rows.empty:
        # Iterate through each matching row
        for index, row in filtered_rows.iterrows():
            status_value = row['Status']
            # Check if status is "Open"
            if status_value == "Open":
                return True  # Return True if any row has "Status" = "Open"
        # If none of the matching rows have "Status" = "Open"
        return False
    else:
        # Return False if no matching rows are found
        return False


def check_if_has_past_trades(tracking_pd, name):
    # Check if any rows contain the specified name in the 'Name' column and have 'Status' as 'Open'
    if any((tracking_pd['Name'] == name) & (tracking_pd['Status'] == 'Closed')):
        return True
    else:
        return False


def check_if_cooldown_period_over(cooldown_until, reformatted_loop_datetime_str):
    # Parse the reformatted_loop_datetime_str into a datetime.date object
    loop_date = datetime.strptime(reformatted_loop_datetime_str, "%Y-%m-%d").date()

    # Compare loop date with cooldown_until
    if cooldown_until < loop_date:
        return True
    else:
        return False


def scope_exchanges(daily_panda):
    for index, daily_panda_row in daily_panda.iterrows():
        coin = daily_panda_row['Coin']
        trades_on_coinbase = check_if_on_coinbase(coin)
        trades_on_kraken = check_if_on_kraken(coin)

        if trades_on_coinbase is False and trades_on_kraken is False:
            daily_panda = daily_panda.drop(index)

    return daily_panda


def check_last_sell_date(tracking_pd, name):
    # Filter the DataFrame for rows where column 'Name' equals the specified name
    filtered_df = tracking_pd[tracking_pd['Name'] == name]

    # Check if any rows match the specified name
    if filtered_df.empty:
        return f"No sales recorded for {name}."

    # Drop rows where 'SaleDate' column contains NaN values
    filtered_df = filtered_df.dropna(subset=['SaleDate'])

    # Check if any valid sales dates remain after dropping NaN values
    if filtered_df.empty:
        return f"No valid sales recorded for {name}."

    # Convert 'SaleDate' column to datetime objects
    filtered_df['SaleDate'] = pd.to_datetime(filtered_df['SaleDate'])

    # Find the newest date in the filtered DataFrame
    newest_date = filtered_df['SaleDate'].max()

    # Convert newest_date to datetime.date object for comparison
    newest_date_date = newest_date.date()

    return newest_date_date


def apply_rank_cutoff(daily_panda):
    for index, daily_panda_row in daily_panda.iterrows():
        rank = daily_panda_row['CMC_Rank']
        makes_rank_cutoff = check_rank(rank)

        if makes_rank_cutoff is False:
            daily_panda = daily_panda.drop(index)

    return daily_panda


def get_daily_file_location(date):
    datetime_without_time = date.date()
    reformatted_date = datetime_without_time.strftime("%m-%d-%Y")
    excel_filename = "E:/Dropbox/CC/Bots/~Files/CMCDataDownloader2Outputs/CMCOutput - " + str(
        reformatted_date) + ".xlsx"

    return excel_filename


def create_tracking_panda():
    df = pd.read_excel(r'.\OpenTrades.xlsx', header=0)

    return df


def check_if_on_coinbase(coin):
    # string to search in file
    with open(r'E:\Dropbox\CC\Bots\~Files\coinbase.txt', 'r') as fp:
        # read all lines using readline()
        lines = fp.readlines()
        for row in lines:
            # check if string present on a current line
            word = coin
            # print(row.find(word))
            # find() method returns -1 if the value is not found,
            # if found it returns index of the first occurrence of the substring
            if row.find(word) != -1:
                # print(coin, 'exists in file')
                return True
            else:
                # print(coin, 'does not exist in file')
                return False


def check_if_on_kraken(coin):
    # string to search in file
    with open(r'E:\Dropbox\CC\Bots\~Files\kraken.txt', 'r') as fp:
        # read all lines using readline()
        lines = fp.readlines()
        for row in lines:
            # check if string present on a current line
            word = coin
            # print(row.find(word))
            # find() method returns -1 if the value is not found,
            # if found it returns index of the first occurrence of the substring
            if row.find(word) != -1:
                # print(coin, 'exists in file')
                return True
            else:
                # print(coin, 'does not exist in file')
                return False


def compare_dates(current_date, past_date):
    date_format = '%Y-%m-%d'

    if isinstance(past_date, pd.Series):
        past_date = past_date.iloc[0]

    past_date = pd.to_datetime(str(past_date), format=date_format).date()
    current_date = pd.to_datetime(str(current_date), format=date_format).date()

    time_difference = (current_date - past_date).days

    return time_difference


def check_rank(current_rank):
    if max_rank > current_rank:
        return True
    return False


def dump_to_excel(data_panda):
    today_date = time.strftime("%m-%d-%Y")
    excel_filename = "E:/Dropbox/CC/Bots/~Files/RankingThresholdWatcher - Lookback/RTW - " + today_date + ".xlsx"

    try:
        data_panda.to_excel(excel_filename, sheet_name='sheet1', index=False)
    except:
        excel_filename = "Results - " + today_date + ".xlsx"
        data_panda.to_excel(excel_filename, sheet_name='sheet1', index=False)
        print("FILE OPEN, CLOSE AND RENAME")


main()
