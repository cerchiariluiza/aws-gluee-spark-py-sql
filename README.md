# aws-gluee-spark-py-sql

socios\
    .select('nome_do_socio_ou_razao_social', 'faixa_etaria', f.dayofmonth('data_de_entrada_sociedade').alias('day_de_entrada'))\
    .where('day_de_entrada == 5')\
    .orderBy('day_de_entrada', ascending=True)\
    .show()
    
    
    empresas\
    .select('cnpj_basico', 'porte_da_empresa', 'capital_social_da_empresa')\
    .groupBy('porte_da_empresa')\
    .agg(
        f.sum("capital_social_da_empresa").alias("capital_social_total"),
        f.count("cnpj_basico").alias("frequencia")
    )\
    .orderBy('porte_da_empresa', ascending=True)\
    .show()  
