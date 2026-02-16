# iedr_pipeline
Esource_assignment

First build the folder structure in git, further enhanced it with baranches

now thnking about data model

we will have brnze where i will simply consume the files
silver will be cleaned data and load data in datamodel (thinking of data model)
and gold will be aggregated lawyer

AI help to find bronze layer DDL as per file format and manually checked if good

leveraged Ai to create boiler plate code for reading csv and loading the datai ndelta table

F.input_file_name failed so used DB diagnosis AI to find other function to make sure audit column have file name

changed the approcah we will use csv and loadit in unified bronze layer and store csv in archive later

now created config table simple with utility, type, source column, target column but when reading form csv we need ot have dedicated schema so added datatype to config tables

--------------

moving to another approcah keep bronze as it is much closer to file and we will rename csv header to match wiht bronze layer, took help of Ai for creatign ddl and config table

handling null in data had issue while ocnversion of data
