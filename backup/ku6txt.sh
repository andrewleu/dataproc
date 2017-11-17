#!/bin/sh
#sed -i "s/Infinity/0.0/g" `grep Infinity -rl /home/mysql/data/tempku6`
#sed -i "s/N\/A/-/g" `grep "N/A"  -rl /home/mysql/data/tempku6`
sed -i "s/Infinity/0.0/g" /home/mysql/data/tempku6
sed -i "s/N\/A/-/g" /home/mysql/data/tempku6
sed -i "s/NaN/0/g" /home/mysql/data/tempku6
