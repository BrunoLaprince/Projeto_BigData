#===================================================
# 5. Análise simples (Big Data na prática)
#===================================================

#===================================================
# Total de vendas por restaurante
#===================================================

print("\nTotal de vendas por restaurante:")
df.groupBy("restaurante") \
  .agg(count("*").alias("QTD_Vendas")) \
  .show()

#===================================================
# Valor médio das vendas
#===================================================

print("\nValor médio das vendas:")
#df.select(avg("valor_pedido", 2).alias("media_vendas")).show()
df.select(round(avg("valor_pedido"), 2).alias("media_vendas")).show()

#===================================================
#  Total de pedidos por cidade
#===================================================
pedidos_cidade = df.groupBy("cidade").agg(
    count("*").alias("total_pedidos"),
    round(sum("valor_pedido"),2).alias("valor_total"),
    round(avg("tempo_entrega_min"),2).alias("tempo_medio_entrega")
)

pedidos_cidade.show()

#===================================================
# Desempenho por restaurante
#===================================================
restaurantes = df.groupBy("restaurante").agg(
    count("*").alias("qtd_pedidos"),
    avg("avaliacao_cliente").alias("avaliacao_media"),
    avg("tempo_entrega_min").alias("tempo_medio")
)

restaurantes.show()

#===================================================    
# Finalizando o spark
#===================================================

spark.stop()