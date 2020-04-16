if [ $# -ne 2 ];then
	echo $0 g1 g2
	exit
fi


g1_path=$1
g2_path=$2

./obtain_inter_data ${g1_path} ${g2_path}

mv ${g1_path}/new_container.pool ${g1_path}/container.pool
mv ${g1_path}/new.meta ${g1_path}/recipes/bv0.meta
mv ${g1_path}/new.recipe ${g1_path}/recipes/bv0.recipe


./restore_identified_file ${g1_path} ${g2_path}
./restore_migriated_file ${g1_path} ${g2_path}
 
rm ${g1_path}/similar_file
rm ${g1_path}/identified_file

./obtain_inter_data ${g2_path} ${g1_path}

mv ${g2_path}/new_container.pool ${g2_path}/container.pool
mv ${g2_path}/new.meta ${g2_path}/recipes/bv0.meta
mv ${g2_path}/new.recipe ${g2_path}/recipes/bv0.recipe


./restore_identified_file ${g2_path} ${g1_path}
./restore_migriated_file ${g2_path} ${g1_path}

rm ${g2_path}/similar_file
rm ${g2_path}/identified_file
