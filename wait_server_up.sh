while ! echo exit | nc -vz localhost 9090; do sleep 10; done
python3 main.py