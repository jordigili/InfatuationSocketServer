echo 'check if ready'
if ! echo exit | nc -vz event_server 9090; then
  echo Fail
else
  echo Ok
fi