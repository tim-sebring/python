import time

def rt(mytime):
    """
    Converts a unix date (epoch time) to standard format, or vice versa depending on
    the input format
    """
    if ":" in str(mytime):
        # contains time too
        pattern = '%m/%d/%Y %H:%M:%S'   # date should be 10/30/2015 05:00:00
        epoch = int(time.mktime(time.strptime(mytime, pattern)))
        return epoch

    elif "/" in str(mytime):
        # doesn't have time, but still a 10/20/2015 date
        pattern = '%m/%d/%Y'
        epoch = int(time.mktime(time.strptime(mytime, pattern)))
        return epoch
    else:  # must be epoch time since there is no punctuation
        newdate = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime(int(mytime)))
        return newdate
