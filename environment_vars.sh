DLT_SRC_BRONZE_ID=$(cat install.log | sed -n -e 's/^.*ID: //p' | sed 's/.$//g')
DLT_SRC_BRONZE_SCHEDULE=$(date -u -d "+10min" +"00 %M %H %d %b ?")