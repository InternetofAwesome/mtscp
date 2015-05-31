#!/bin/bash 

# MINSIZE=$((50*1024))
MINSIZE=0
THREADS=6
BLOCK_SIZE=1024

pids=""
fds=""
start_time=0

function finish {
	killall ssh
	killall dd
	for i in $(seq 0 $((THREADS-1))); do
		rm ${fds[$i]}
	done
	echo "Done"
}
trap finish EXIT

running()
{
	for i in $(seq 0 $((THREADS-1))); do
		if ps --pid ${pids[i]} > /dev/null; then
			return 0
		fi
	done
	return 1
}

get_transferred()
{
	transferred=0
	for i in $(seq 0 $((THREADS-1))); do
		kill -USR1  ${pids[i]} 2> /dev/null
		sz=$(cat ${fds[i]} | tail -n1 | sed -n 's/\([0-9]\+\) bytes.*/\1/p')
		# echo "transferred size is ($sz)"
		transferred=$((transferred+sz))
	done
	echo $transferred
}


size=$(du -b $1 | awk '{print $1}')
echo file size: $size
BLOCK_SIZE=$((size/THREADS))
echo block size: $BLOCK_SIZE
echo ssh root@s.samsurl.com "rm $2 -f; dd if=/dev/zero of=$2 bs=$BLOCK_SIZE count=$THREADS"
exit
if(($size > $MINSIZE)); then
	#calculate how many BLOCK_SIZE blocks there are in the file. 
	blocks=$((size/BLOCK_SIZE))
	#calculate how many blocks each file chunk will be
	chunk_size=$((blocks/THREADS))
	echo "$blocks total blocks with chunk size of $chunk_size blocks"
	for i in $(seq 0 $((THREADS-1))); do
		#calculate the starting block offset to read/write to
		start=$((chunk_size*i))
		#make a temp file to act as the fifo buffer
		fds[$i]=`mktemp`
		#make a fifo buffer to hold output from dd.
		mkfifo $fds[$i]
		if((i==THREADS-1)); then
			dd if="$1" bs=$BLOCK_SIZE skip=$start > >(ssh root@s.samsurl.com "dd of="$2" bs=$BLOCK_SIZE seek=$start") 2> ${fds[i]} &
		else
			dd if="$1" bs=$BLOCK_SIZE skip=$start count=$chunk_size > >(ssh root@s.samsurl.com "dd of="$2" bs=$BLOCK_SIZE seek=$start count=$chunk_size ") 2> ${fds[i]} &
		fi
		#get the PID of the process just spawned
		pids[$i]=$!
		# echo "got pid ${pids[i]}"
	done
	ps -AF | grep `which dd`
	
	echo ps -AF --pid ${pids[0]}
	start_xfer=$(get_transferred)
	start_time=$(date +%s)
	while running; do
		sleep 1
		now=$(date +%s)
		elapsed=$((now - start_time))
		total_xfer=$(get_transferred - start_xfer)
		echo -e "\r\033[K$((total_xfer/1024/1024)) MB / $((size/1024/1024)) MB - $((total_xfer/elapsed/1024)) kB/s"
	done
else
	scp $1 $2
fi
