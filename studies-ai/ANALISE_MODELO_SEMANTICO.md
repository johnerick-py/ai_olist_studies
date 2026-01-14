# 📊 Análise do Modelo Semântico Olist

## Visão Geral
Modelo dimensional baseado em dados da **Olist**, maior plataforma de e-commerce do Brasil. Este é um modelo **star schema** clássico com 1 tabela de fatos e 4 dimensões.

---

## 🏗️ Estrutura do Modelo

### Tabelas

#### **fact_order** (Tabela de Fatos)
- **Descrição**: Contém informações de pedidos com dados de vendas, preços, fretes e avaliações de clientes de cada item de pedido
- **Registros**: Cada linha representa um item dentro de um pedido
- **Colunas principais**:
  - `order_id` - Identificador único do pedido
  - `order_item_id` - Número sequencial do item no pedido
  - `customer_id` - Referência ao cliente
  - `seller_id` - Referência ao vendedor
  - `product_id` - Referência ao produto
  - `order_date_key` - Data do pedido
  - `item_price` - Preço do item vendido
  - `shipping_cost` - Custo de frete para entrega
  - `order_status` - Status atual do pedido (entregue, cancelado, etc)
  - `review_score` - Nota de avaliação do cliente (1-5 estrelas)
  - `review_comment` - Comentário da avaliação

#### **dim_customer** (Dimensão de Clientes)
- **Descrição**: Informações demográficas e geográficas dos clientes da plataforma Olist
- **Chave primária**: `customer_id`
- **Colunas principais**:
  - `customer_id` - Identificador do cliente no pedido
  - `customer_unique_id` - ID único do cliente (cliente pode ter múltiplas contas)
  - `customer_city` - Cidade onde o cliente está localizado
  - `customer_state` - Estado/UF onde o cliente está localizado
  - `customer_zip_code_prefix` - Prefixo do CEP

#### **dim_seller** (Dimensão de Vendedores)
- **Descrição**: Informações sobre os vendedores parceiros da plataforma
- **Chave primária**: `seller_id`
- **Registros**: 3.095 vendedores únicos

#### **dim_product** (Dimensão de Produtos)
- **Descrição**: Características dos produtos vendidos como categoria, peso, dimensões e número de fotos
- **Chave primária**: `product_id`
- **Colunas principais**:
  - `product_id` - Identificador do produto
  - `product_category_name` - Categoria principal do produto
  - `product_name_length` - Comprimento do nome (caracteres)
  - `product_description_length` - Comprimento da descrição
  - `product_photos_qty` - Número de fotos do produto
  - `product_weight_g` - Peso em gramas
  - `product_length_cm`, `product_height_cm`, `product_width_cm` - Dimensões

#### **dim_time** (Dimensão de Tempo)
- **Descrição**: Calendário e atributos temporais para análise de vendas por período
- **Chave primária**: `date_key`

---

## 📊 Medidas Criadas

### Medidas de Volume
| Medida | Descrição | Fórmula |
|--------|-----------|---------|
| **Total Orders** | Número total de pedidos únicos | `DISTINCTCOUNT(fact_order[order_id])` |
| **Total Items Sold** | Número total de itens vendidos | `COUNTA(fact_order[order_item_id])` |
| **Unique Customers** | Número de clientes únicos | `DISTINCTCOUNT(fact_order[customer_id])` |
| **Unique Sellers** | Número de vendedores únicos | `DISTINCTCOUNT(fact_order[seller_id])` |
| **Unique Products Sold** | Número de produtos únicos vendidos | `DISTINCTCOUNT(fact_order[product_id])` |

### Medidas Financeiras
| Medida | Descrição | Fórmula |
|--------|-----------|---------|
| **Total Revenue** | Receita total de vendas | `SUMX(fact_order, fact_order[item_price])` |
| **Total Shipping Cost** | Custo total com frete | `SUM(fact_order[shipping_cost])` |
| **Gross Profit** | Lucro bruto (receita - frete) | `[Total Revenue] - [Total Shipping Cost]` |
| **Average Order Value** | Valor médio por pedido | `DIVIDE([Total Revenue], [Total Orders])` |
| **Average Item Price** | Preço médio por item | `DIVIDE([Total Revenue], [Total Items Sold])` |

### Medidas de Performance
| Medida | Descrição | Fórmula |
|--------|-----------|---------|
| **Gross Margin %** | Margem de lucro bruta (lucro/receita) | `DIVIDE([Gross Profit], [Total Revenue])` |
| **Delivery Success Rate** | Percentual de pedidos entregues com sucesso | `DIVIDE(CALCULATE(COUNTROWS(fact_order), fact_order[order_status]="delivered"), [Total Orders])` |
| **Average Review Score** | Nota média de avaliação dos produtos | `AVERAGEX(fact_order, VALUE(fact_order[review_score]))` |

---

## 🔗 Relacionamentos

O modelo segue um padrão **star schema** com a tabela de fatos no centro:

```
         dim_customer
              ↑
              │ customer_id
              │
dim_seller ← fact_order → dim_product
              │
              │ order_date_key
              ↓
          dim_time
```

| De | Para | Campo | Tipo |
|----|------|-------|------|
| fact_order | dim_customer | customer_id | Many-to-One |
| fact_order | dim_seller | seller_id | Many-to-One |
| fact_order | dim_product | product_id | Many-to-One |
| fact_order | dim_time | order_date_key | Many-to-One |

---

## 📈 Casos de Uso

Este modelo é ideal para análises como:

1. **Análise de Vendas**
   - Receita por período, categoria, região
   - Desempenho de vendedores
   - Comparação de períodos

2. **Análise de Clientes**
   - Distribuição geográfica de clientes
   - Valor médio do cliente
   - Taxa de pedidos por estado

3. **Análise de Produtos**
   - Categorias mais vendidas
   - Correlação entre características do produto e vendas
   - Avaliações por categoria

4. **Qualidade e Logística**
   - Taxa de sucesso de entregas
   - Custo médio de frete
   - Avaliações dos clientes por entrega

5. **Performance de Negócio**
   - Margem bruta
   - Itens vendidos vs. receita
   - Ticket médio

---

## 🗄️ Fonte de Dados

- **Origem**: OneLink (Data Lake do Fabric)
- **Esquema**: `olist_dimensional`
- **Modo**: DirectLake (conexão dinâmica, sem cache)
- **Tabelas base**: 
  - olist_orders_dataset
  - olist_order_items_dataset
  - olist_customers_dataset
  - olist_sellers_dataset
  - olist_products_dataset
  - olist_order_reviews_dataset

---

## 📝 Notas

- O modelo utiliza **Direct Lake** para máxima performance e atualização em tempo real
- Todas as descrições das tabelas e colunas foram adicionadas para facilitar o entendimento
- As medidas foram criadas com nomes e descrições em português para melhor compreensão
- O modelo é otimizado para análises de e-commerce com foco em vendas, satisfação do cliente e performance logística

