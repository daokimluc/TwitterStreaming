#!/bin/sh
python twitterToSpark.py &
python sparkProcess.py &
python visualize/app.py &