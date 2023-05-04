# # Python Tools : Stats Desc 
# 
# Author : François LORTHIOIR
# 
# file History : 
# - 08/08/2021 v10 : creation of module
# 
# Content : 
# - Descriptive Stats for an SQL table
# - Descriptive Stats for a Pandas dataframe
#
# Copyright : Published and shared under the Apache License
#    Please feel free to use an modify provided you are keeping this copyright and 
#    this all header section, including the author name and email. 
#    any issue please feel free to inform me at fran325@gmail.com
#    Will eventually be published in github of equivalent
#     
# %%
import pandas as pd
# %% [markdown]
# ##  Fonctions de Statistiques Descriptives
# %% [markdown]
# ### pour Dataframe Pandas

# %%
## Descriptive Stats for quantitive variables (Measures)

def  get_stats_quanti(df):
    
    """
    Generates descriptive stats for quantitative variables :
    type, min, max, mean, std, median, rate of na's, variation rate (std / mean)
    
    input : dataframe with quantitative variables
    output : a dataframe with the stats 
    
    """
    types = pd.DataFrame(df.dtypes, columns = ['dtype'])
    stats1 = df.agg(['min', 'max', 'mean', 'std', 'median']).transpose()
    stats2 = df.agg([lambda x: x.isna().mean(),
                  lambda x: x.std() / x.mean()]).transpose()
    stats2.columns = ['taux_na', 'taux_var']
    return types.join(stats1).join(stats2)
 


# %%
## Descriptive Stats for qualitative variables (Dimensions)

def get_stats_quali(df):
    
    """
    Generates descriptive stats for quantitative variables :
    Type, nb of unique values, min, max, rate of na's 
    
    input : dataframe with qualitative variables
    output : a dataframe with the stats 
     """
    
    types = pd.DataFrame(df.dtypes, columns = ['dtype'])
    stats1 = df.agg(['nunique', 'min', 'max']).transpose()
    stats2 = pd.DataFrame(df.agg(lambda x: x.isna().mean()), columns = ['taux_na'])
    
    
    return types.join(stats1).join(stats2)

# %% [markdown]
# ### pour tables Postgres

# %%
def get_stats_sql_postgres(dbConnection, table_schema, table_name, quanti_var = None, quali_var = None):
    """
    Get Descriptive stats from an SQL table. A connexion to a database needs to be established into dbConnexion variable
    using SQLAlchemy.
    
    In parameters : 
    table_name : name of table to get stats from
    schema_name : name of schema 
    quanti_var : a list of columns, will be processed as quantitative variables 
        stats include : min, max, mean, std, NA_rate
    quali_var : a list of columns, will be processed as qualitative variables
        stats include : min, max, NA_rate, nunique : nb of distinct values
        
    Generates a dataframe from table description and stats
    
    Returns : a dataframe with stats, message
    
    """
    
    # Initialisations
    
    quanti_query = """
        SELECT '{col}' AS Field, 
        MIN({col}) AS Min, 
        MAX({col}) AS Max, 
        AVG({col}) AS Mean, 
        STDDEV(CAST({col} AS REAL)) AS Std, 
        (COUNT(*) - COUNT({col})) AS Nb_NA,
        NULL
    FROM {schema}.{table}

    """
    
    
    quali_query = """
        SELECT '{col}' AS Field, 
        MIN({col}) AS Min, 
        MAX({col}) AS Max, 
        NULL, 
        NULL, 
        (COUNT(*) - COUNT({col})) AS Nb_NA,
        COUNT(DISTINCT {col}) AS Nb_val
    FROM {schema}.{table}

    """
    
    
    # Getting DESCRIBE of table in dataframe desc_df (mysql)
    
    #query = "DESCRIBE {table}".format(table = table_name)
    #try:
    #    desc_table = dbConnection.execute(query).fetchall()
        
    #except Exception as ex:
    #    print(ex)
    #    return
    
    
    #desc_df = pd.DataFrame(desc_table, columns = ['Field', 'Type', 'Null','Key', 'Default', 'Comments'])

    # Getting description of table (postgres)

    query = """
        SELECT 
            column_name, 
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
        ORDER BY ordinal_position
        """.format(table = table_name, schema = table_schema)

    try:
         desc_table = dbConnection.execute(query).fetchall()
        
    except Exception as ex:
         print(ex)
         return
    
    desc_df = pd.DataFrame(desc_table, columns = ['Field', 'Type', 'Null', 'Default'])
    
    # looping on column names to get stats in stats_df
    
    stats_list = []
    print('Compute stats on table : ', table_name)
    
    for col in desc_df['Field']: 
        
        if col in quanti_var:
            # Processing quantitative variables
            query = quanti_query.format(col = col, table = table_name, schema = table_schema)
            print('*** for column : ', col)
            try:
                quanti_row = dbConnection.execute(query).fetchall()
            except Exception as ex:
                print(ex)
                return
        
            stats_list = stats_list + quanti_row
            
            
        if col in quali_var:
            # Processing qualitative variables
            query = quali_query.format(col = col, table = table_name, schema = table_schema)
            print('*** for column : ', col)
            try:
                quali_row = dbConnection.execute(query).fetchall()
            except Exception as ex:
                print(ex)
                return
        
            stats_list = stats_list + quali_row            
            
    stats_df = pd.DataFrame(stats_list, columns = ['Field', 'Min', 'Max','Mean', 'Std', 'Nb_NA', 'Nb_val'])   
    
    # Returning join of desc_df and stats_df
    
    return desc_df.merge(stats_df, how = 'inner', on = 'Field')

