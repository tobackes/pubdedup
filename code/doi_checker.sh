id_file=$1; #/data_ssd/backests/Repositories/pubdedup/labelled_publication_mentions.db;
doi_folder=$2; #/data_ssd/backests/Repositories/pubdedup/dois/;

batchsize=1000;

# First, add the DOIs from the mentions database to the dois database as additional lines in mentions with code and legal being null.
# Then, run this script.

#sqlite3 ${id_file} "INSERT OR IGNORE INTO types SELECT doi,NULL FROM dois WHERE code IS NULL";

maximum=`sqlite3 ${id_file} "SELECT COUNT(DISTINCT doi) FROM dois WHERE code IS null"`;

N=256
for ((start=0; start<=${maximum}+${batchsize}; start=start+${batchsize})); do
    ((i=i%N)); ((i++==0)) && wait
    sqlite3 $id_file "select distinct doi from dois where code is null order by doi limit ${start},${batchsize}" | while IFS= read -r line; do echo $line `curl -Is https://doi.org/${line} | head -1`; done > ${doi_folder}/${start}.txt &
done
