./build/titandb_bench --db="/home/titan/build/test_db" --wal_dir="/home/titan/build/test_wal" --benchmarks=fillrandom --compression_type=none --value_size=50000 --use_titan=true --num=1000000 --use_existing_db=0

./build/titandb_bench --db="/home/titan/build/test_db" --wal_dir="/home/titan/build/test_wal" --benchmarks=readseq --compression_type=none --value_size=50000 --use_titan=true --num=1000000 --use_existing_db=1

./build/titandb_bench --db="/home/titan/build/test_db" --wal_dir="/home/titan/build/test_wal" --benchmarks=readunorder --compression_type=none --value_size=50000 --use_titan=true --num=1000000 --use_existing_db=1