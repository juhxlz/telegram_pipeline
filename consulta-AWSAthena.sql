#Consultas realizadas no AWS Athena utilizando o SQL

# 1. Número de mensagens enviadas a cada dia:

SELECT
  context_date,
  count(1) AS "message_amount"
FROM "telegram"
GROUP BY context_date
ORDER BY context_date DESC;

# 2. Número de mensagens por usuário, por dia:

SELECT
  user_id,
  user_first_name,
  context_date,
  count(1) AS "message_amount"
FROM "telegram"
GROUP BY
  user_id,
  user_first_name,
  context_date
ORDER BY context_date DESC

# 3. Tamanho médio das mensagens enviadas por cada usuário em cada dia:

SELECT
  user_id,
  user_first_name,
  context_date,
  CAST(AVG(length(text)) AS INT) AS "average_message_length"
FROM "telegram"
GROUP BY
  user_id,
  user_first_name,
  context_date
ORDER BY context_date DESC;

# 4. Número de mensagens por hora, dia da semana e número da semana.

WITH
parsed_date_cte AS (
    SELECT
        *,
        CAST(date_format(from_unixtime("date"),'%Y-%m-%d %H:%i:%s') AS timestamp) AS parsed_date
    FROM "telegram"
),
hour_week_cte AS (
    SELECT
        *,
        EXTRACT(hour FROM parsed_date) AS parsed_date_hour,
        EXTRACT(dow FROM parsed_date) AS parsed_date_weekday,
        EXTRACT(week FROM parsed_date) AS parsed_date_weeknum
    FROM parsed_date_cte
)
SELECT
    parsed_date_hour,
    parsed_date_weekday,
    parsed_date_weeknum,
    count(1) AS "message_amount"
FROM hour_week_cte
GROUP BY
    parsed_date_hour,
    parsed_date_weekday,
    parsed_date_weeknum
ORDER BY
    parsed_date_weeknum,
    parsed_date_weekday

# 5. Top 3 palavras mais frequentes

WITH word_split AS ( SELECT
        text, word FROM
        telegram,
        UNNEST(SPLIT(text, ' ')) AS t(word)
         WHERE LOWER(word) NOT IN ('meu', 'minha', 'de', 'a', 'o', 'qual', 'uma', 'está', 'em', 'como', 'que')
)
SELECT word, COUNT(*) AS word_count FROM word_split GROUP BY word ORDER BY word_count DESC LIMIT 3;
