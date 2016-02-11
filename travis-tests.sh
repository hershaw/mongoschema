cd mongoschema

nohup python flask_test_server.py > server.log 2>&1 &

# give it a second to start
sleep 2

python test.py
