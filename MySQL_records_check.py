import time
from datetime import datetime
import mysql.connector
import threading
import telebot

# ---------------- CONFIG START ----------------
# Telegram bot credentials (this is not real ID and TOKENs, just for example)
TELEGRAM_CHAT_ID = 423709163
TELEGRAM_DEAL_BOT_TOKEN = "14953984759:AAGl3wfhkdmjXJYLbHtVvfaF2efgvDnHvQ"
TELEGRAM_ERROR_BOT_TOKEN = "5909480231:AAH7xvWo6yMzH7QqZGllOMdmjXJYLbH-Vv0"

dealBot = telebot.TeleBot(TELEGRAM_DEAL_BOT_TOKEN)
errorBot = telebot.TeleBot(TELEGRAM_ERROR_BOT_TOKEN)

# Database configuration
DB_CONFIG = {
	'host': 'localhost',
	'user': 'robot',
	'password': 'hlOMdmjXJ-tVvfa',
	'database': 'Delisting'
	}
# ---------------- CONFIG END ----------------

def check_DB_for_coins_to_trade():
	"""
	Periodically checks the database for coins that need to be traded.
	If 2 or more records are found, they are sent for processing, and
	the status in the database is updated to mark them as processed.
	"""
	print(f'[Coins_DB_check] Start checking DB for coins to trade   {datetime.now()}')
	while True:
		connection = None	# connection variable initialization 
		cursor = None		# cursor variable initialization 
		try:
			function_start_time = time.time()				# milestone to measure time spent for function work
			with mysql.connector.connect(**DB_CONFIG) as connection:	# start connection to Database (will be closed automatically cause use "with" method)
				with connection.cursor(dictionary=True) as cursor:	# start cursor (will be closed automatically cause use "with" method)
					try:
						cursor.execute("LOCK TABLES coinsBybit WRITE")		# lock the table to avoid race conditions
						cursor.execute("SELECT * FROM coinsBybit")		# fetch all rows from the coins table
						rows = cursor.fetchall()

						coins_to_trade = []					# coins that haven't been traded yet
						coins_already_traded = []				# coins already processed earlier

						for row in rows:
							if row['Already_traded'] == 0:			# searching for coins with "Already_traded = FALSE" status, that should be send to separate thread to start deal
								coins_to_trade.append(row)
							else:
								coins_already_traded.append(row)	# coins with status "Already_traded = TRUE", that are already sent for deal time before

						if coins_to_trade:
							for coin in coins_to_trade:			# FOR loop to start deals for coins with status "Already_traded = FALSE"  
								assetName = coin['assetName']		# extract coin name
								current_price = float(coin['price'])	# extract coin price for trade volume calculation

								#--- Update coin status in DB to prevent repeated processing of the same coin
								cursor.execute("UPDATE coinsBybit SET Already_traded = TRUE WHERE assetName = %s", (assetName,))			# updating coin status to "Already_traded = TRUE"			
								if cursor.rowcount > 0:													# checking whether status was changed
									print(f'[Coins_DB_check {assetName}] Status updated to Already_traded = TRUE   {datetime.now()}')
								else:
									print(f"[Coins_DB_check {assetName}] -- WARNING -- Status was NOT updated   {datetime.now()}")
									#--- message for Telegram is sending in separate thread to not delay main function work
									message = f"{assetName} Warning: Status was not updated in DB   {datetime.now()}"				# message creation
									threading.Thread(target=send_telegram_message, args=(errorBot, TELEGRAM_CHAT_ID, message), daemon=True).start()	# sending of the message
									continue	# skipping this coin and going to the next one in the FOR loop

								connection.commit()	# commit DB update
								#--- Sending coin's data by separate thread to the function for trades order placement
								print(f"\n--- Starting trade thread for {assetName} ---   {datetime.now()}\n")
								threading.Thread(target=tradeBybit, args=(assetName, current_price,), daemon=True).start()
								print(f"[Coins_DB_check {assetName}] Time spent: {(time.time() - function_start_time) * 1000} ms   {datetime.now()}")

					except Exception as e:			# catching Database work error
						print(f"[Coins_DB_check] Cursor error: {str(e)}  {datetime.now()}")
					finally:
						cursor.execute("UNLOCK TABLES")	# release Database lock

		except mysql.connector.Error as e:				# catching Database connector error
			print(f"\n[Coins_DB_check] MySQL error: {str(e)}   {datetime.now()}")
			#--- message for Telegram is sending in separate thread to not delay main function work
			message = f"MySQL Warning: Error while checking coins   {datetime.now()}"
			threading.Thread(target=send_telegram_message, args=(errorBot, TELEGRAM_CHAT_ID, message), daemon=True).start()

		except Exception as e:						# catching other errors
			print(f'[Coins_DB_check] General error: {str(e)}   {datetime.now()}')
			#--- message for Telegram is sending in separate thread to not delay main function work
			message = f"General Warning: Error while checking coins   {datetime.now()}"
			threading.Thread(target=send_telegram_message, args=(errorBot, TELEGRAM_CHAT_ID, message), daemon=True).start()

		finally:		# closing cursor and connection to Database (not neseccary because of "with" method used), but keeped from previous version
			if cursor:
				cursor.close()
			if connection and connection.is_connected():
				connection.close()

		time.sleep(0.05)	# short delay before next Database check

def send_telegram_message(bot, chat_id, message):
	"""
	Sends a message to Telegram using a separate thread and try-except method
	"""
	try:					
		bot.send_message(chat_id=chat_id, text=message)
		print(f"[send_telegram_message] Message sent: {message}   {datetime.now()}")
	except Exception as e:	# catching errors if so
		print(f"[send_telegram_message] Error: {str(e)}   {datetime.now()}")
