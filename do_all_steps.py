import polygon_api_downloader as polygon_api_downloader
import polygon_api_stock_events_downloader as polygon_api_stock_events_downloader
import calculate_indicators as calculate_indicators
import data_merger as data_merger
import add_volume_profile as add_volume_profile
import candle_dataset_builder as candle_dataset_builder
import candle_long_term_dataset_builder as candle_long_term_dataset_builder

if __name__ == "__main__": 
    
    # download last price history
    polygon_api_downloader.do_step() 

    # download stock events
    polygon_api_stock_events_downloader.do_step()

    # merge data
    data_merger.do_step()

    # calculate indicators
    calculate_indicators.do_step()

    # add volume profile
    add_volume_profile.do_step()

    # build dataset for each ticker 
    candle_dataset_builder.do_step()

    # build long term dataset for each ticker
    candle_long_term_dataset_builder.do_step()

    print("Done.")

    


