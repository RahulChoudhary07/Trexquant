import logging

from itch_processor import NasdaqITCHProcessor

if __name__ == "__main__":
    file_path = input("Please enter the path of the NASDAQ ITCH 5.0 tick data zip file: ")
    
    itch_processor = NasdaqITCHProcessor()
    itch_processor.get_hourly_VWAP(file_path)
    logging.info("Done!!!")




