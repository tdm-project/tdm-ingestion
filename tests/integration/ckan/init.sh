#!/usr/bin/env bash
while [ $(docker inspect -f '{{.State.Running}}' ckan) = false ]; do  docker restart ckan; done

sleep 10
docker exec ckan /usr/local/bin/ckan-paster --plugin=ckan datastore set-permissions -c /etc/ckan/production.ini | docker exec -i db psql -U ckan
sleep 5
docker exec ckan bash -c ". /usr/lib/ckan/venv/bin/activate; cd /usr/lib/ckan/venv/src/ckan; paster create-test-data user -c /etc/ckan/production.ini; paster sysadmin add  tester -c  /etc/ckan/production.ini; paster create-test-data family -c /etc/ckan/production.ini"
docker restart ckan
