#! /bin/sh

if [ $# -ne 2 ]; then
	echo "error"
fi

echo $1 $2

starttime=`date +'%Y-%m-%d %H:%M:%S'`

./obtain_inter_data $1 $2
echo ""

./restore_identified_file $1 $2
echo ""

./restore_migriated_file $1 $2
echo ""

mv $1/new.meta $1/recipes/bv0.meta 
mv $1/new.recipe $1/recipes/bv0.recipe

echo ""
echo ""
echo ""
echo ""

./obtain_inter_data $2 $1
echo ""

./restore_identified_file $2 $1
echo ""

./restore_migriated_file $2 $1
echo ""

mv $2/new.meta $2/recipes/bv0.meta 
mv $2/new.recipe $2/recipes/bv0.recipe

echo ""
endtime=`date +'%Y-%m-%d %H:%M:%S'`
echo "total cost:" "$((end_seconds-start_seconds))"s


echo ""
echo ""

./get_total_data_size $1 $2

