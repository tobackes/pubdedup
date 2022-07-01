attach database "dois_v2" as dois;

select * from mentions where goldID not in (select doi from dois.dois where legal) or goldID in (select doi from dois.types where type not in ('book-chapter','disseration','monograph','journal-article','proceedings-article','report')) limit 10;
