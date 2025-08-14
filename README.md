# Apache-Metadata-Pipeline
Walks a dataset directory holding parquet files, generates Apache Hadoop Catalog and corresponding tables of metadata. Deploys Apache Superset server, Postgres server (holds Superset metadata), and Spark Thrift Query Engine in individual containers.


# If your parquet files contain incompatible data types with iceberg vectorization reads (like UINT64), must cast data to compatible type:
    # Activate virtual environment:
    /opt/homebrew/bin/python3.12 -m venv .venv
    source .venv/bin/activate

    # Install PyArrow depedency:
    pip install --upgrade pip
    pip install pyarrow==16.1.0

    # Run sanitize script:

    python sanitize_parquet.py \
      --in ./data/<Insert_Parquet_File_Name> \
      --out ./<Insert_Output_Directory>/Insert_Parquet_File_Name \

    Example:
    python sanitize_parquet.py \
      --in ./data/System_Interface_Counters \
      --out ./data_casted/System_Interface_Counters \


    (If this step is done, must replace the unsanitized data with the generated sanitized data)


# Prerequisites:
    - Create and populate data directory
        - Inside data directory will be folders (representing different tables) holding your raw parquet files that will be aggregated to their respective tables
    
    - Create empty warehouse directory
        - This will hold your table's metadata

# Run Project:
    docker-compose up -d --build


    This runs various servers in individual contaienrs:
    - Postgres server (stores Superset's metadata)
    - Superset server (your dashboard)
    - Spark Thrift server (your query engine) 
    
