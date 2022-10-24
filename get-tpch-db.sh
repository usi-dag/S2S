if [ ! -d "TPCH-sqlite" ]; then
  echo "cloning TPCH-sqlite"
  git clone --recursive git@github.com:flpo/TPCH-sqlite.git
fi

cd TPCH-sqlite

make clean
SCALE_FACTOR="${SCALE_FACTOR:-0.01}" make
status=$?
if [ $status -eq 0 ]; then
  cp TPC-H.db TPC-H-$SCALE_FACTOR.db
else
  echo "Error generating the database"
fi