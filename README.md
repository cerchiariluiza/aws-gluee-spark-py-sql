# aws-gluee-spark-py-sql

socios\
    .select('nome_do_socio_ou_razao_social', 'faixa_etaria', f.dayofmonth('data_de_entrada_sociedade').alias('day_de_entrada'))\
    .where('day_de_entrada == 5')\
    .orderBy('day_de_entrada', ascending=True)\
    .show()
