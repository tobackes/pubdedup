select freq, count(freq),group_concat(minel) from (select minel,count(minel) as freq from mentionIDIndex2minel group by minel) group by freq order by freq;
select freq,count(*) from (select count(*) as freq from label2mentionID group by label) group by freq;

select freq,count(*) from (select count(*) as freq from minel2label group by label) group by freq;

attach database "representations_institutions_v2/bielefeld.db" as mentions;
attach database "../instipro_v2/representations/6/institutions/bielefeld.db" as mentions;
attach database "representations_institutions_v2/features.db" as features;
attach database "representations_institutions_v2/representations.db" as reps;

select string from mentions.representations where mentionID=6400316;

select featGroup,feat from index2feat where featIndex in (select featIndex from features.features where repIDIndex in (select minel from minel2crossfreq order by freq desc limit 3,1));

# histogram of component sizes
select count(*),freq,label from (select count(*) as freq,label from components group by label) group by freq;

# attach required databases
attach database "/data_ssds/disk04/backests/pubdedup/representations_publications/restrictions_publications_stricter/4261_3021_4441_0_20/features.db" as feats;
attach database "/data_ssds/disk04/backests/pubdedup/representations_publications/restrictions_publications_stricter/4261_3021_4441_0_20/representations.db" as reps;

attach database "/data_ssds/disk06/backests/pubdedup/representations_publications/restrictions_publications_ngrams/no_generalization/features.db" as feats;
attach database "/data_ssds/disk06/backests/pubdedup/representations_publications/restrictions_publications_ngrams/no_generalization/representations.db" as reps;

attach database "/data_ssds/disk01/backests/pubdedup/representations_authors/restrictions_authors_4_names/no_generalization/features.db" as feats;
attach database "/data_ssds/disk01/backests/pubdedup/representations_authors/restrictions_authors_4_names/no_generalization/representations.db" as reps;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# get freq of certain minels
select freq from reps.representations where repID in (select repID from feats.index2repID where repIDIndex=74010801) limit 1;
# get features of certain repIDIndex
select featGroup||":"||feat from feats.index2feat where featIndex in (select featIndex from feats.features where repIDIndex=74010801);
# get number of repIDs per minel
select count(*), freq, minel from (select count(*) as freq,minel from repIDIndex2minel  where minel in (select minel from minel2label where label=1312) group by minel) group by freq;
# get number of minels per repID
select count(*), freq, repIDIndex from (select count(*) as freq,repIDIndex from repIDIndex2minel  where minel in (select minel from minel2label where label=1312) group by repIDIndex) group by freq;
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# check that the representations database attached is the right one > one check is not sufficient!!
select * from reps.representations where repID in (select repID from features.index2repID where repIDIndex in (select minel from minel2label where label=2577910)) limit 1;

# analyse the minel structure in terms of how many specification they have in the bipartite graph
select count(*), freq from (select count(*) as freq from repIDIndex2minel  where minel in (select minel from minel2label where label=1312) group by repIDIndex) group by freq;

# histogram over the minel sizes of one component
select freq,count(*) from (select count(*) as freq,minel from repIDIndex2minel where minel in (select minel from minel2label where label=2541630) group by minel) group by freq;

# find minels that have more than 10 representations underneath and sufficient features and are less frequent than 100
select * from reps.representations where repID in (select repID from features.index2repID where repIDIndex in (select minel from (select count(*) as freq,minel from repIDIndex2minel where minel in (select minel from minel2label where label=2541630) group by minel) where freq > 10 group by freq)) and a1sur is not null and term1 is not null and year1 is not null and freq<100;

# create temporary table for the above first step
create temporary table large_minels as select repID from features.index2repID where repIDIndex in (select minel from (select count(*) as freq,minel from repIDIndex2minel where minel in (select minel from minel2label where label=1) group by minel) where freq > 100 group by freq);

# above second step using temporary table
select * from reps.representations inner join large_minels on large_minels.repID=reps.representations.repID where reps.representations.a1sur is not null and reps.representations.term1 is not null and reps.representations.year1 is not null and reps.representations.freq<10000;

# this is to be used on the labelled_mentions.db to find false-negatives
select freq,goldID,repIDs from (select goldID,count(distinct label) as freq,group_concat(distinct repID) as repIDs from mentions group by goldID) where freq>1 limit 0,10;

# THE BELOW IS ACTUALLY FOR SPECIFICATION
CREATE TEMPORARY TABLE underspecified AS SELECT repID AS repID,(a1sur IS NULL AND a2sur IS NULL AND a3sur IS NULL AND a4sur IS NULL) AS surname, (term1 IS NULL AND term2 IS NULL AND term3 IS NULL AND term4 IS NULL) AS term, (year1 IS NULL AND year2 IS NULL) AS year FROM representations WHERE (a1sur IS NULL AND a2sur IS NULL AND a3sur IS NULL AND a4sur IS NULL) OR (term1 IS NULL AND term2 IS NULL AND term3 IS NULL AND term4 IS NULL) OR (year1 IS NULL AND year2 IS NULL);
UPDATE representations SET a1sur=repID WHERE repID IN (SELECT repID FROM underspecified WHERE surname=1);
UPDATE representations SET term1=repID WHERE repID IN (SELECT repID FROM underspecified WHERE term=1 AND surname=0);
UPDATE representations SET year1=repID WHERE repID IN (SELECT repID FROM underspecified WHERE year=1 AND term=0 AND surname=0);

#Trying to get counts from names like tables 9, 10 in reference paper
select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1                   from mentions where goldID is not null) group by l,l_,f1                  ) group by freq;
select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1,f2,f3             from mentions where goldID is not null) group by l,l_,f1,f2,f3            ) group by freq;
select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1,f1_,f2,f2_,f3,f3_ from mentions where goldID is not null) group by l,l_,f1,f1_,f2,f2_,f3,f3_) group by freq;

select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1                   from mentions where goldID is not null) group by goldID) group by freq;
select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1,f2,f3             from mentions where goldID is not null) group by goldID) group by freq;
select freq, count(*) from (select count(*) as freq from (select distinct goldID,l,l_,f1,f1_,f2,f2_,f3,f3_ from mentions where goldID is not null) group by goldID) group by freq;

select sum(num_ments) from name_info_2 inner join onekplus_used on name_info_2.superblock=onekplus_used.superblock where dist_rids>0;
select sum(num_ments) from name_info_2 where num_rids>0 and superblock in (select superblock from (select superblock,sum(num_ments) as size,sum(num_rids) as annotations from name_info_2 where superblock is not null group by superblock) where size > 1000 and annotations >0);
