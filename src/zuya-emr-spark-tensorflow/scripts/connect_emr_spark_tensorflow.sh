MASTER_IP=35.165.22.180
PORT=$1

ssh -i ~/.ssh/zuya_galvanize01.pem -NL $PORT:localhost:$PORT hadoop@$MASTER_IP
