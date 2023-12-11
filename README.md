# Trexquant

## VWAP - NASDAQ ITCH 5.0

Calculate the Volume weighted average price of each stock at all trading hours till market close given the NASDAQ ITCH 5.0 tick data file.

## Getting Started

### Prerequisites

```
pip install -r requirements.txt
```

## Running 

```
1. python main.py
2. Enter the complete path of the NASDAQ ITCH 5.0 tick data zip file 
```

## NASDAQ ITCH 5 Data file

Download the zip file using the below link

https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

## Output

`/VWAP_output_hourly` folder has VWAP results for each trading hour, and the files are CSVs which are named by the timestamp.

## Sample Output

`/Sample_VWAP_output_hourly` folder has VWAP results for each trading hour, generated when I ran the code on my end.
