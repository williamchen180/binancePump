
from python_ntfy import NtfyClient
import pandas as pd
import numpy as np
import json
import datetime as dt
import operator
from binance.enums import *
from binance import ThreadedWebsocketManager
from pricechange import *
from binanceHelper import *
from pricegroup import *
import signal
import threading
import sys
import argparse
from typing import Dict, List
from loguru import logger
import os
import time

show_only_pair = "USDT" #Select nothing for all, only selected currency will be shown
show_limit = 1      #minimum top query limit
min_perc = 0.05     #min percentage change
price_changes: List[PriceChange] = []
price_groups: Dict[str, PriceGroup] = {}
last_symbol = "X"
chat_ids = []
twm: ThreadedWebsocketManager

# Global variables for ntfy and logging
ntfy_client = None
state_file = "binancePump_state.json"
save_timer = None

def setup_logging():
    """Setup loguru logging with daily log files"""
    # Remove default handler
    logger.remove()
    
    # Add file handler with daily rotation
    # Use dynamic filename pattern that includes date
    log_file_pattern = "binancePump_{time:YYYY_MM_DD}.log"
    
    # Add file logger with rotation at midnight
    logger.add(
        log_file_pattern,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        rotation="00:00",  # Rotate at midnight
        retention="30 days"
    )
    
    # Add console logger
    logger.add(
        sys.stdout,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )

def setup_ntfy(channel_name):
    """Setup ntfy client"""
    global ntfy_client
    ntfy_client = NtfyClient(topic=channel_name)
    logger.info(f"ntfy initialized with topic: {channel_name}")

def serialize_data():
    """Serialize current state data to JSON format"""
    try:
        # Convert PriceChange objects to dictionaries
        price_changes_data = []
        for pc in price_changes:
            price_changes_data.append({
                'symbol': pc.symbol,
                'prev_price': pc.prev_price,
                'price': pc.price,
                'total_trades': pc.total_trades,
                'open_price': pc.open_price,
                'volume': pc.volume,
                'is_printed': pc.is_printed,
                'event_time': pc.event_time.isoformat(),
                'prev_volume': pc.prev_volume
            })
        
        # Convert PriceGroup objects to dictionaries
        price_groups_data = {}
        for symbol, pg in price_groups.items():
            price_groups_data[symbol] = {
                'symbol': pg.symbol,
                'tick_count': pg.tick_count,
                'total_price_change': pg.total_price_change,
                'relative_price_change': pg.relative_price_change,
                'total_volume_change': pg.total_volume_change,
                'last_price': pg.last_price,
                'last_event_time': pg.last_event_time.isoformat(),
                'open_price': pg.open_price,
                'volume': pg.volume,
                'is_printed': pg.is_printed
            }
        
        # Create complete state data
        state_data = {
            'timestamp': dt.datetime.now().isoformat(),
            'price_changes': price_changes_data,
            'price_groups': price_groups_data,
            'last_symbol': last_symbol,
            'show_only_pair': show_only_pair,
            'show_limit': show_limit,
            'min_perc': min_perc
        }
        
        return state_data
    except Exception as e:
        logger.error(f"Failed to serialize data: {e}")
        return None

def deserialize_data(state_data):
    """Deserialize state data from JSON format"""
    global price_changes, price_groups, last_symbol, show_only_pair, show_limit, min_perc
    
    try:
        # Check if data is too old (more than 10 minutes)
        saved_time = dt.datetime.fromisoformat(state_data['timestamp'])
        current_time = dt.datetime.now()
        if (current_time - saved_time).total_seconds() > 600:  # 10 minutes
            logger.info("Saved data is too old (>10 minutes), discarding")
            return False
        
        # Restore price_changes
        price_changes.clear()
        for pc_data in state_data['price_changes']:
            pc = PriceChange(
                symbol=pc_data['symbol'],
                prev_price=pc_data['prev_price'],
                price=pc_data['price'],
                total_trades=pc_data['total_trades'],
                open_price=pc_data['open_price'],
                volume=pc_data['volume'],
                is_printed=pc_data['is_printed'],
                event_time=dt.datetime.fromisoformat(pc_data['event_time']),
                prev_volume=pc_data['prev_volume']
            )
            price_changes.append(pc)
        
        # Restore price_groups
        price_groups.clear()
        for symbol, pg_data in state_data['price_groups'].items():
            pg = PriceGroup(
                symbol=pg_data['symbol'],
                tick_count=pg_data['tick_count'],
                total_price_change=pg_data['total_price_change'],
                relative_price_change=pg_data['relative_price_change'],
                total_volume_change=pg_data['total_volume_change'],
                last_price=pg_data['last_price'],
                last_event_time=dt.datetime.fromisoformat(pg_data['last_event_time']),
                open_price=pg_data['open_price'],
                volume=pg_data['volume'],
                is_printed=pg_data['is_printed']
            )
            price_groups[symbol] = pg
        
        # Restore other global variables
        last_symbol = state_data.get('last_symbol', 'X')
        show_only_pair = state_data.get('show_only_pair', 'USDT')
        show_limit = state_data.get('show_limit', 1)
        min_perc = state_data.get('min_perc', 0.05)
        
        logger.info(f"Successfully restored state: {len(price_changes)} price changes, {len(price_groups)} price groups")
        return True
        
    except Exception as e:
        logger.error(f"Failed to deserialize data: {e}")
        return False

def save_state():
    """Save current state to file"""
    try:
        state_data = serialize_data()
        if state_data:
            with open(state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
            logger.info(f"State saved to {state_file}")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_state():
    """Load state from file if it exists and is valid"""
    try:
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                state_data = json.load(f)
            
            if deserialize_data(state_data):
                logger.info("State restored successfully")
                return True
            else:
                logger.info("State file discarded due to age or corruption")
                return False
        else:
            logger.info("No state file found, starting fresh")
            return False
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
        return False

def periodic_save():
    """Periodic save function called every 10 minutes"""
    global save_timer
    save_state()
    # Schedule next save in 10 minutes
    save_timer = threading.Timer(600.0, periodic_save)
    save_timer.daemon = True
    save_timer.start()

def send_notification_and_log(title, message):
    """Send notification via ntfy and log the message"""
    # Print to console (keep original behavior)
    print(f"{message}")
    
    # Log the message
    logger.info(f"{title}: {message}")
    
    # Send ntfy notification
    if ntfy_client:
        try:
            pass
            # ntfy_client.send(message=message, title=title)
        except Exception as e:
            logger.error(f"Failed to send ntfy notification: {e}")

def get_price_groups() -> List[PriceGroup]:
    """
    Returns a snapshot list of all current PriceGroup objects.
    """
    return list(price_groups.values())


def process_message(tickers):
    for ticker in tickers:
        symbol = ticker['s']

        if not show_only_pair in symbol:
            continue

        price = float(ticker['c'])
        total_trades = int(ticker['n'])
        open = float(ticker['o'])
        volume = float(ticker['v'])
        event_time = dt.datetime.fromtimestamp(int(ticker['E'])/1000)
        if len(price_changes) > 0:
            price_change = filter(lambda item: item.symbol == symbol, price_changes)
            price_change = list(price_change)
            if (len(price_change) > 0):
                price_change = price_change[0]
                price_change.event_time = event_time
                price_change.prev_price = price_change.price
                price_change.prev_volume = price_change.volume
                price_change.price = price
                price_change.total_trades = total_trades
                price_change.open = open
                price_change.volume = volume
                price_change.is_printed = False
            else:
                price_changes.append(PriceChange(symbol, price, price, total_trades, open, volume, False, event_time, volume))
        else:
            price_changes.append(PriceChange(symbol, price, price, total_trades, open, volume, False, event_time, volume))

    price_changes.sort(key=operator.attrgetter('price_change_perc'), reverse=True)
    
    for price_change in price_changes:
        if (not price_change.is_printed 
            and abs(price_change.price_change_perc) > min_perc 
            and price_change.volume_change_perc > min_perc):

            price_change.is_printed = True 

            if not price_change.symbol in price_groups:
                price_groups[price_change.symbol] = PriceGroup(price_change.symbol,                                                                
                                                            1,                                                                
                                                            abs(price_change.price_change_perc),
                                                            price_change.price_change_perc,
                                                            price_change.volume_change_perc,                                                                
                                                            price_change.price,                                                                                                                             
                                                            price_change.event_time,
                                                            price_change.open_price,
                                                            price_change.volume,
                                                            False,
                                                            )
            else:
                price_groups[price_change.symbol].tick_count += 1
                price_groups[price_change.symbol].last_event_time = price_change.event_time
                price_groups[price_change.symbol].volume = price_change.volume
                price_groups[price_change.symbol].last_price = price_change.price
                price_groups[price_change.symbol].is_printed = False
                price_groups[price_change.symbol].total_price_change += abs(price_change.price_change_perc)
                price_groups[price_change.symbol].relative_price_change += price_change.price_change_perc
                price_groups[price_change.symbol].total_volume_change += price_change.volume_change_perc                

    if len(price_groups)>0:
        anyPrinted = False 
        sorted_price_group = sorted(price_groups, key=lambda k:price_groups[k]['tick_count'])
        if (len(sorted_price_group)>0):
            sorted_price_group = list(reversed(sorted_price_group))
            for s in range(show_limit):
                header_printed=False
                if (s<len(sorted_price_group)):
                    max_price_group = sorted_price_group[s]
                    max_price_group = price_groups[max_price_group]
                    if not max_price_group.is_printed:
                        msg = "Top Ticks"
                        if not header_printed:
                            print(msg)
                            header_printed = True
                        send_notification_and_log(msg, max_price_group.to_string(True))
                        anyPrinted = True

        sorted_price_group = sorted(price_groups, key=lambda k:price_groups[k]['total_price_change'])
        if (len(sorted_price_group)>0):
            sorted_price_group = list(reversed(sorted_price_group))
            for s in range(show_limit):
                header_printed=False
                if (s<len(sorted_price_group)):
                    max_price_group = sorted_price_group[s]
                    max_price_group = price_groups[max_price_group]
                    if not max_price_group.is_printed:
                        msg = "Top Total Price Change"
                        if not header_printed:
                            print(msg)
                            header_printed = True
                        send_notification_and_log(msg, max_price_group.to_string(True))
                        anyPrinted = True

        sorted_price_group = sorted(price_groups, key=lambda k:abs(price_groups[k]['relative_price_change']))
        if (len(sorted_price_group)>0):
            sorted_price_group = list(reversed(sorted_price_group))
            for s in range(show_limit):
                header_printed=False
                if (s<len(sorted_price_group)):
                    max_price_group = sorted_price_group[s]
                    max_price_group = price_groups[max_price_group]
                    if not max_price_group.is_printed:
                        msg = "Top Relative Price Change"
                        if not header_printed:
                            print(msg)
                            header_printed = True
                        send_notification_and_log(msg, max_price_group.to_string(True))
                        anyPrinted = True

        sorted_price_group = sorted(price_groups, key=lambda k:price_groups[k]['total_volume_change'])
        if (len(sorted_price_group)>0):
            sorted_price_group = list(reversed(sorted_price_group))
            for s in range(show_limit):
                header_printed=False
                if (s<len(sorted_price_group)):
                    max_price_group = sorted_price_group[s]
                    max_price_group = price_groups[max_price_group]
                    if not max_price_group.is_printed:
                        msg = "Top Total Volume Change"
                        if not header_printed:
                            print(msg)
                            header_printed = True
                        send_notification_and_log(msg, max_price_group.to_string(True))
                        anyPrinted = True

        if anyPrinted:
            print("")

def stop():
    twm.stop()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Binance Pump Detector with ntfy notifications')
    parser.add_argument('--ntfy', default='BinancePump-9527', help='ntfy channel name (default: BinancePump-9527)')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger.info("BinancePump started")
    
    # Load previous state if available
    load_state()
    
    # Setup ntfy
    setup_ntfy(args.ntfy)
    
    # Start periodic save timer (every 10 minutes)
    periodic_save()
    
    #READ API CONFIG
    api_config = {}
    with open('api_config.json') as json_data:
        api_config = json.load(json_data)
        json_data.close()

    api_key = api_config['api_key']
    api_secret = api_config['api_secret']
    
#    Create an Event that signals “stop everything”
    stop_event = threading.Event()

    # Define our signal handler to set that event
    def handle_exit(signum, frame):
        print("\nShutting down…")
        logger.info("Received shutdown signal, saving state...")
        save_state()  # Save state before exit
        if save_timer:
            save_timer.cancel()  # Cancel periodic save timer
        stop_event.set()
        twm.stop()        # tells the Binance manager to stop

    # Catch SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # Start the threaded websocket manager
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    twm.start_ticker_socket(process_message)
    
    print("Websocket running. Press Ctrl+C to exit.")

    # Now simply wait until we’re told to stop…
    stop_event.wait()

    print("Clean exit complete.")
    return
    
if __name__ == '__main__':
    main()
