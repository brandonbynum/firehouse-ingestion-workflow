import time

def convert_str_12hr_to_24hr(twelve_hour: str):
    input_format = "%I:%M %p" if "am" in twelve_hour.lower() or "pm" in twelve_hour.lower() else "%I:%M"
    output_format = "%H:%M"
    
    twelve_hour_datetime = time.strptime(twelve_hour, input_format)
    twentyfour_hour_str = time.strftime(output_format, twelve_hour_datetime)
    return twentyfour_hour_str
