#!/bin/sh

#
# basic map-reduce test
#

# run the test in a fresh sub-directory.
rm -rf tmp && mkdir tmp

# compile code
make lab1 || exit 1

# generate the correct output
bin/mrsequential bin/plugins/wc.so data/*.txt || exit 1
sort mr-out-0 > tmp/mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s bin/mrmaster data/*.txt &

# give the master time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s bin/mrworker bin/plugins/wc.so &
timeout -k 2s 180s bin/mrworker bin/plugins/wc.so &
timeout -k 2s 180s bin/mrworker bin/plugins/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait


# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.
failed_any=0

sort mr-out* | grep . > tmp/mr-wc-all
if cmp tmp/mr-wc-all tmp/mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait ; wait ; wait


# now indexer
rm -rf mr-*

# generate the correct output
bin/mrsequential bin/plugins/indexer.so data/*.txt || exit 1
sort mr-out-0 > tmp/mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s bin/mrmaster data/*.txt &
sleep 1

# start multiple workers
timeout -k 2s 180s bin/mrworker bin/plugins/indexer.so &
timeout -k 2s 180s bin/mrworker bin/plugins/indexer.so

sort mr-out* | grep . > tmp/mr-indexer-all
if cmp tmp/mr-indexer-all tmp/mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting map parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s bin/mrmaster data/*.txt &
sleep 1

timeout -k 2s 180s bin/mrworker bin/plugins/mtiming.so &
timeout -k 2s 180s bin/mrworker bin/plugins/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting reduce parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s bin/mrmaster data/*.txt &
sleep 1

timeout -k 2s 180s bin/mrworker bin/plugins/rtiming.so &
timeout -k 2s 180s bin/mrworker bin/plugins/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait ; wait


# generate the correct output
bin/mrsequential bin/plugins/nocrash.so data/*.txt || exit 1
sort mr-out-0 > tmp/mr-correct-crash.txt
rm -f mr-out*

echo '***' Starting crash test.

rm -f tmp/mr-done
(timeout -k 2s 180s bin/mrmaster data/*.txt ; touch tmp/mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s bin/mrworker bin/plugins/crash.so &

# mimic rpc.go's masterSock()
SOCKNAME=824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f tmp/mr-done ]
  do
    timeout -k 2s 180s bin/mrworker bin/plugins/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f tmp/mr-done ]
  do
    timeout -k 2s 180s bin/mrworker bin/plugins/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f tmp/mr-done ]
do
  timeout -k 2s 180s bin/mrworker bin/plugins/crash.so
  sleep 1
done

wait
wait
wait

rm $SOCKNAME
sort mr-out* | grep . > tmp/mr-crash-all
if cmp tmp/mr-crash-all tmp/mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
