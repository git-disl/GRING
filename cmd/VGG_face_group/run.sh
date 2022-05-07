# If you use run.sh to launch many peer nodes, you should comment below line
#client.lib.Input() in peer.py
#Input() in peer.go

NUM_NODES=25
START_ADDR=130.127.133.15
START_PORT=8000
#SERVER_ADDR=54.90.248.24
SERVER_ADDR=130.127.133.15
SERVER_PORT=9999


for i in `seq 1 $NUM_NODES`
do
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 150 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT &
    go run peer.go $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 50 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    sleep 4
done

START_PORT=8026
for i in `seq 1 $NUM_NODES`
do
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 300 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT &
    go run peer.go $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 100 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    sleep 4
done

START_PORT=8051
for i in `seq 1 $NUM_NODES`
do
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 480 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT &
    go run peer.go $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 200 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    sleep 4
done

START_PORT=8076
for i in `seq 1 $NUM_NODES`
do
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 600 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    #python3 peer.py $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT &
    go run peer.go $START_ADDR $(($i + $START_PORT)) $SERVER_ADDR:$SERVER_PORT 300 0 | tee node$(($i + $START_PORT)).log 2>&1 &
    sleep 4
done
