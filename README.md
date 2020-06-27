# Fraudulent Events Detection

This is the code of a streaming application implemented in scala using Flink that detects fraudulent events on a website. Those events are composed of clicks on ads and displays of ads.
Those events are simulated using Kafka 
## Offline Analysis

In order to have an idea of the data, we have analysed data with an offline method from a JSON file that we have extract from the stream.

We have performed an analysis on uid and IP address and we have noticed that the data behave differently.

You can find this analysis on a python notebook in the directory “offline_analysis”. 

## Clicks and displays fraud detection with Flink

In order to detect fraud, we have implemented two patterns detection, one based on CTR (click trough rate) and one based on the time between a display and a click.

The normal CTR is around 10% so we have set that if the CTR is beyond 20% then it is a fraud (only if there are at least 3 clicks, because it often happens that if a user clicks only once then the CTR is 100%). In fact, if a user clicks often on every ads, it is highly likely that the user is in fact a bot.

The second method is the average time spends between a display and a click by user. We have set that, if a user clicks very fast on ads, then it will be highly likely that the user is actually a bot.

We have two ways to identify a user, the user Id and the Ip address. So we have implemented the two patterns detection on uid and ip.

Nevertheless, the timestamp is not always in ascending order because event can be broadcast with lateness. That is why we have used Watermark to solve this issue because the lateness in the data could led to miscalculation with the Window function.

## Code

You can find the main implementation on FraudDetectionJob.scala and the functions used to detect pattern in AnomalyDetection.scala. Watermark function is code in TimestampExtractor.scala

Furthermore, we have tried to implement the pattern of mean time between each click. This try is implemented in the file FraudDetector.scala. 

## Fraudulent Events output to files

Every fraudulent event are stored in specific files for every pattern (4 patterns : 2 for CTR per uid and per ip and 2 for Mean time between click and display per uid and per ip)
Just launch FraudDetectionJob.scala to create those files.
