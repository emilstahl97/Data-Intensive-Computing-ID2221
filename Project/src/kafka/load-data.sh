#!/bin/bash

#unzip all data
unzip ../../Wiki-pageviews/2015/July/pagecounts-20150701-170000.zip -d ../../Wiki-pageviews/2015/July/
unzip ../../Wiki-pageviews/2015/July/pagecounts-20150702-170000.zip -d ../../Wiki-pageviews/2015/July/

unzip ../../Wiki-pageviews/2015/August/pagecounts-20150801-170000.zip -d ../../Wiki-pageviews/2015/August/
unzip ../../Wiki-pageviews/2015/August/pagecounts-20150802-170000.zip -d ../../Wiki-pageviews/2015/August/

unzip ../../Wiki-pageviews/2016/July/pagecounts-20160701-170000.zip -d ../../Wiki-pageviews/2016/July/
unzip ../../Wiki-pageviews/2016/July/pagecounts-20160702-170000.zip -d ../../Wiki-pageviews/2016/July/

unzip ../../Wiki-pageviews/2016/August/pagecounts-20160801-170000.zip -d ../../Wiki-pageviews/2016/August/
unzip ../../Wiki-pageviews/2016/August/pagecounts-20160802-170000.zip -d ../../Wiki-pageviews/2016/August/

# read into kafka

bash read-into-kafka.sh 2015-jul-1 ../../Wiki-pageviews/2015/July/pagecounts-20150701-170000
bash read-into-kafka.sh 2015-jul-2 ../../Wiki-pageviews/2015/July/pagecounts-20150702-170000

bash read-into-kafka.sh 2015-aug-1 ../../Wiki-pageviews/2015/Aug/pagecounts-20150801-170000
bash read-into-kafka.sh 2015-aug-2 ../../Wiki-pageviews/2015/Aug/pagecounts-20150802-170000

bash read-into-kafka.sh 2016-jul-1 ../../Wiki-pageviews/2016/July/pagecounts-20160701-170000
bash read-into-kafka.sh 2016-jul-2 ../../Wiki-pageviews/2016/July/pagecounts-20160702-170000

bash read-into-kafka.sh 2016-aug-1 ../../Wiki-pageviews/2016/Aug/pagecounts-20160801-170000
bash read-into-kafka.sh 2016-aug-2 ../../Wiki-pageviews/2016/Aug/pagecounts-20160802-170000

