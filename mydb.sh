#!/bin/bash
cd 
cd /home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update
echo "Loading Data in Database"
cat outfile.txt  | grep '^Insert' > ever.txt

#cat outfile.txt  | grep '^Insert' > ever.txt
# grep "^Insert" '/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/output.txt' > '/home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/ever.txt'
# mysql -u root -pcmVwc3J2ZmVjMjAxMQ < /home/app/data/suppression_temp/python_revenueupdate/myEnv/revenue_update/outfile.txt
echo "Loading Data in Database completed"

# cat abc.txt  | grep '^Insert' > newfile