import gzip
import pandas as pd
import logging
from utils import output_df_as_csv


class NasdaqITCHProcessor:
    def __init__(self):
        """
        Initialize NasdaqITCHProcessor with necessary attributes.
        """
        self.buy_orders = {}  # order_id: [price, qty, locate_id]
        self.stock_names = {}  # locate_id: symbol
        self.executed_orders = {}  # locate_id: [price, qty, order_id, match_id]
        self.system = {}  # event_code: timestamp
        self.count_hour_delta = 0
        self.timestamp = 0 # timestamp of the latest message data
        self.parsers = {
            "S": self.parse_system_event_message,
            "R": self.parse_stock_dictionary,
            "H": self.parse_stock_trading_action,
            "Y": self.parse_reg_sho_short_sale_price,
            "L": self.parse_market_participant_position,
            "V": self.parse_mwcb_decline_level_message,
            "W": self.parse_mwcb_status_message,
            "K": self.parse_ipo_quoting_period_update,
            "J": self.parse_limit_up_down_auction_collar,
            "h": self.parse_operational_halt,
            "A": self.parse_add_order_with_and_without_mpid_attribution,
            "F": self.parse_add_order_with_and_without_mpid_attribution,
            "E": self.parse_order_executed_message,
            "C": self.parse_order_executed_with_price_message,
            "X": self.parse_order_cancel_message,
            "D": self.parse_order_delete_message,
            "U": self.parse_order_replace_message,
            "P": self.parse_non_cross_trade_message,
            "Q": self.parse_cross_trade_message,
            "B": self.parse_broken_trade_execution_message,
            "I": self.parse_noii_message,
        }
    
    @staticmethod
    def extract_timestamp(message: bytes)->int:
        return int.from_bytes(message[5:11], byteorder='big')

    def parse_system_event_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)
        event_code = chr(message[11])
        self.system[event_code] = self.timestamp
        return None

    def parse_stock_dictionary(self, message: bytes)->None:
        locate_id = int.from_bytes(message[1:3],'big')
        stock_name = message[11:19].decode('ascii','ignore').strip()
        self.stock_names[locate_id]= stock_name
        self.executed_orders[locate_id]=[]
        self.timestamp =  self.extract_timestamp(message)

    def parse_stock_trading_action(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)

    def parse_reg_sho_short_sale_price(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)
        
    def parse_market_participant_position(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)
        
    def parse_mwcb_decline_level_message(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)
        
    def parse_mwcb_status_message(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)

    def parse_ipo_quoting_period_update(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)
        
    def parse_limit_up_down_auction_collar(self, message: bytes)->None:
        self.timestamp =  self.extract_timestamp(message)

    def parse_operational_halt(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)

    def parse_add_order_with_and_without_mpid_attribution(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)
        locate_id = int.from_bytes(message[1:3], byteorder='big')
        order_id=int.from_bytes(message[11:19], byteorder='big')
        qty=int.from_bytes(message[20:24], byteorder='big')
        price=int.from_bytes(message[32:36], byteorder='big')
        self.buy_orders[order_id] = [price, qty, locate_id]
        return None

    def parse_order_executed_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)
        locate_id = int.from_bytes(message[1:3], byteorder='big')
        order_id=int.from_bytes(message[11:19], byteorder='big')
        qty=int.from_bytes(message[19:23], byteorder='big')
        match_id=int.from_bytes(message[23:31], byteorder='big')
        try:
            price, qty_, _ = self.buy_orders.get(order_id)
            if qty_ > qty:
                self.buy_orders.update({order_id: [price, qty_ - qty, locate_id]})
            else:
                del self.buy_orders[order_id]

            
            self.executed_orders[locate_id] = [[price, qty, 0, match_id]]
            
        except:
            return None
        return None

    def parse_order_executed_with_price_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)
    
        if chr(message[31]) == 'N': return None

        locate_id = int.from_bytes(message[1:3], byteorder='big')
        order_id=int.from_bytes(message[11:19], byteorder='big')
        qty=int.from_bytes(message[19:23], byteorder='big')
        match_id=int.from_bytes(message[23:31], byteorder='big')
        price=int.from_bytes(message[32:36], byteorder='big')        
        try:
            price_, qty_, _ = self.buy_orders.get(order_id)
            if qty_ > qty:
                self.buy_orders.update({order_id: [price_, qty_ - qty, locate_id]})
            else:
                del self.buy_orders[order_id]
            
            self.executed_orders[locate_id] = [[price, qty, 0, match_id]]
            
        except:
            return None
        return None
        
    def parse_order_cancel_message(self, message: bytes)->None:
        self.timestamp=self.extract_timestamp(message)
        order_id=int.from_bytes(message[11:19], byteorder='big')
        qty=int.from_bytes(message[19:23], byteorder='big')

        if self.buy_orders.get(order_id) is not None:
            self.buy_orders[order_id][1]=self.buy_orders[order_id][1]-qty
            if self.buy_orders[order_id][1] <=0:
                del self.buy_orders[order_id]
        else:
            return None
        return None

    def parse_order_delete_message(self, message: bytes)->None:
        self.timestamp=self.extract_timestamp(message)
        self.buy_orders.pop(int.from_bytes(message[11:19],'big'),None)

        return None
        
    def parse_order_replace_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)
        locate_id = int.from_bytes(message[1:3], byteorder='big')
        old_order_id=int.from_bytes(message[11:19], byteorder='big')
        new_order_id=int.from_bytes(message[19:27], byteorder='big')
        qty=int.from_bytes(message[27:31], byteorder='big')
        price=int.from_bytes(message[31:35], byteorder='big') 
        self.buy_orders.pop(old_order_id,None)
        self.buy_orders[new_order_id] = [price, qty, locate_id]
      
        return None
        
    def parse_non_cross_trade_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)

        locate_id = int.from_bytes(message[1:3],'big')
        qty=int.from_bytes(message[20:24], byteorder='big')
        price=int.from_bytes(message[32:36], byteorder='big')
        match_id=int.from_bytes(message[36:44], byteorder='big')
        self.executed_orders[locate_id] = [[price, qty, 0, match_id]]
    
        return None
            
    def parse_cross_trade_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)

        locate_id = int.from_bytes(message[1:3], 'big')
        qty=int.from_bytes(message[11:19], byteorder='big')
        price=int.from_bytes(message[27:31], byteorder='big')
        match_id=int.from_bytes(message[31:39], byteorder='big')
        
        
        self.executed_orders[locate_id] = [[price, qty, 0, match_id]]
    
        return None
        
    def parse_broken_trade_execution_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)

        locate_id = int.from_bytes(message[1:3], 'big')
        match_id = int.from_bytes(message[11:19], 'big')
        try:
            orders = self.executed_orders.get(locate_id)
            if orders is not None:
                new_orders = list(filter(lambda a: not a[3] == match_id, orders))
                self.executed_orders.update({locate_id: new_orders})
        except (TypeError, KeyError):
            return None
        return None

    def parse_noii_message(self, message: bytes)->None:
        self.timestamp = self.extract_timestamp(message)

    def calculate_hourly_VWAP(self)->None:
        """
        Calculate and output hourly VWAP.

        Args:
        timestamp (int): Timestamp of the current event.
        """
        vwaps = []

        for locate_id, trades in self.executed_orders.items():
            volume, volumne_price = 0, 0
            volume = sum(trade[1] for trade in trades)
            volumne_price = sum(trade[0] * trade[1] for trade in trades)
            if volume > 0:
                vwaps.append({"stock_symbol": self.stock_names[locate_id], "VWAP": volumne_price / (volume * 1e4)})
        output_df_as_csv(pd.DataFrame(vwaps), str(self.timestamp))
        logging.info(f"VWAP for trading hour, {str(self.timestamp)}, was calculated")
        self.count_hour_delta += 1

    def get_hourly_VWAP(self, file_path: str)->None:
        """
        Process the NASDAQ ITCH file and calculate hourly VWAP.

        Args:
        file_path (str): Path to the NASDAQ ITCH file.

        """
        start_timestamp = 0
        close_timestamp = 0

        with gzip.open(file_path, "rb") as file_object:
            message_size = int.from_bytes(file_object.read(2), 'big')
            while message_size:
                message = file_object.read(message_size)
                message_type = chr(message[0])
                self.parsers[message_type](message)

                if start_timestamp == 0:
                    try:
                        start_timestamp = self.system["Q"]
                        logging.info('Trading Starts!')
                    except KeyError:
                        pass
                if (start_timestamp != 0) and (self.timestamp - start_timestamp > (3600 * 1e9) * (self.count_hour_delta + 1)):
                    self.calculate_hourly_VWAP()

                if close_timestamp == 0:
                    try:
                        close_timestamp = self.system["M"]
                        self.calculate_hourly_VWAP()
                        logging.info('Trading Ends, VWAP calculation done!')
                        break
                    except KeyError:
                        pass
                message_size = int.from_bytes(file_object.read(2), 'big')
        return None