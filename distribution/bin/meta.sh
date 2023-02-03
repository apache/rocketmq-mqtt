#!/bin/sh

#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one or more
#  * contributor license agreements.  See the NOTICE file distributed with
#  * this work for additional information regarding copyright ownership.
#  * The ASF licenses this file to You under the Apache License, Version 2.0
#  * (the "License"); you may not use this file except in compliance with
#  * the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

if [ -z "$ROCKETMQ_MQTT_HOME" ]; then
  ## resolve links - $0 may be a link to maven's home
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ]; do
    ls=$(ls -ld "$PRG")
    link=$(expr "$ls" : '.*-> \(.*\)$')
    if expr "$link" : '/.*' >/dev/null; then
      PRG="$link"
    else
      PRG="$(dirname "$PRG")/$link"
    fi
  done

  saveddir=$(pwd)

  ROCKETMQ_MQTT_HOME=$(dirname "$PRG")/..

  # make it fully qualified
  ROCKETMQ_MQTT_HOME=$(cd "$ROCKETMQ_MQTT_HOME" && pwd)

  cd "$saveddir"
fi

export ROCKETMQ_MQTT_HOME

BASEDIR=$HOME
mkdir -p $BASEDIR/logs

mainClass="org.apache.rocketmq.mqtt.meta.starter.Startup"


function startup() {
  pid=`ps aux|grep $mainClass|grep -v grep |awk '{print $2}'`
  if [ ! -z "$pid" ]; then
    echo "java is runing..."
    exit 1
  fi
  nohup sh ${ROCKETMQ_MQTT_HOME}/bin/runserver.sh $mainClass $@ >$BASEDIR/logs/start_out.log 2>&1 &
}

function stop() {
  pid=`ps aux|grep $mainClass|grep -v grep |awk '{print $2}'`
  if [ -z "$pid" ]; then
    echo "no java to kill"
  fi
  printf 'stop...'
  kill $pid
  sleep 3
  pid=`ps aux|grep $mainClass|grep -v grep |awk '{print $2}'`

  if [ ! -z $pid ]; then
    kill -9 $pid
  fi
}

case "$1" in
start)
  startup $@
  ;;
stop)
  stop
  ;;
restart)
  stop
  startup
  ;;
*)
  printf "Usage: sh  $0 %s {start|stop|restart}\n"
  exit 1
  ;;
esac