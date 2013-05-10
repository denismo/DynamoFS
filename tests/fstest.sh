#!/bin/bash

function rt {
    echo Executing "$*"
    bash -c "$*"
}

dir=$RANDOM
mkdir ${dir}
cd ${dir}

rt "echo Hello > aaa"
rt mv aaa bbb
rt cp bbb bbb_1
rt cat bbb_1
rt rm -f bbb
rt ln -s bbb_1 bbb
rt ls
rt cat bbb
rt rm -f bbb
rt ln bbb_1 l_bbb
rt cat l_bbb
rt rm -f bbb_1
rt cat l_bbb
rt rm -f l_bbb

rt mkdir d1
rt ln -s d1 s_d1
rt touch d1/aaa
rt cat s_d1/aaa
rt chmod 600 s_d1/aaa
rt "echo Hello > d1/aaa"
rt rm -rf d1
rt rm s_d1

rt mkfifo fifo
rt "echo Hello > fifo&"
rt cat fifo

cd ..
rm -rf ${dir}
