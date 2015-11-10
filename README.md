# naacp
analytical scripts for naacp project

build_all.py - Creates Psql data tables for all data matrices in target directory.
               Table names derived from subdirectory names; col names from file names.
               It's assumed that all data files will be in child directories of target.
               Data files will be non-sparse, all integers, comma-delimited, and
               have both row and column headers.  Rows are origins, cols are destinations.
               
database.py  - Login credentials

build_flat_files.py - Creates flattened (vectorized) version of all files in specified directory.
                      Adds origin, destination fields.  Can be run standalone.  Usage:
                      
                      build_flat_files.build_flat_files(input_directory, output_directory)
                      
build_tables.py - Creates database tables from all flat files in designated directory.  Uses
                  COPY if possible, fails over to INSERTs otherwise (memory constraints are a
                  potential issue).  Usage:
                  
                  build_tables.build_tables(db=database_name, flat_file_directory, drop_old=True)
                  
                  To load a single flat file to a specific table:
                  load_with_insert(database_name, table_name, file_name, drop_old=None)
                  
test_db_loading.py -  Runs tests against simulated data files.                  
                  
