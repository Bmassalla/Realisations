# Databricks notebook source
# MAGIC %md
# MAGIC ## LISTE DES FICHIERS (TABLES) DANS LA BASE

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables"))

# COMMAND ----------

# Spécifiez le chemin complet du fichier à supprimer
file_path = "dbfs:/FileStore/tables/BAC.csv"

# Supprimez le fichier
dbutils.fs.rm(file_path, True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECTION DE LA TABLE (FICHIER) A UTILISER

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/BAC.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # II- TOP10 PAR ORDRE DE MOYENNE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 PREMIERS TOUTE SERIE CONFONDUE

# COMMAND ----------


P10TSC = spark.sql('''
                   SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
ORDER BY MOYENNE DESC
LIMIT 10
                   ''')
P10TSC.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADMIS AVEC LA DEUXIEME MOYENNE LA PLUS ELEVEE

# COMMAND ----------


SHM = spark.sql('''
SELECT BAC.MATRICULE, NOM, PRENOM, SEXE, SERIE, MOYENNE AS SECOND_HIGHEST_AVG, BAC.ETABLISSEMENT
FROM BAC
WHERE MOYENNE = (
    SELECT MAX(MOYENNE)
    FROM BAC
    WHERE MOYENNE < (SELECT MAX(MOYENNE) FROM BAC)
);
               ''')

SHM.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LISTE DES ADMIS AVEC MOYENNE LA PLUS ELEEE PAR SERIE

# COMMAND ----------

MOYSERIE = spark.sql( ''' 
SELECT BAC.NOM, BAC.PRENOM, BAC.SERIE, BAC.MOYENNE
FROM BAC
INNER JOIN (
    SELECT SERIE, MAX(MOYENNE) AS MAX_MOYENNE
    FROM BAC
    GROUP BY SERIE
) AS SUBQUERY
ON BAC.SERIE = SUBQUERY.SERIE AND BAC.MOYENNE = SUBQUERY.MAX_MOYENNE
ORDER BY BAC.MOYENNE DESC 
''')

MOYSERIE.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## TP10 SERIE C

# COMMAND ----------

TOP10C =spark.sql(''' 
                  SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
WHERE SERIE ="C"
ORDER BY MOYENNE DESC
LIMIT 10
                  ''')
TOP10C.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##TOP10 SERIE D

# COMMAND ----------

TOP10D =spark.sql(''' 
                  SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
WHERE SERIE ="D"
ORDER BY MOYENNE DESC
LIMIT 10
                  ''')

TOP10D.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP10 SERIE A4

# COMMAND ----------

TOP10A4=spark.sql(''' 
                  SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
WHERE SERIE ="A4"
ORDER BY MOYENNE DESC
LIMIT 10
                  ''')

TOP10A4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP10 SERIE A2

# COMMAND ----------

TOP10A2=spark.sql('''
                  SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
WHERE SERIE ="A2"
ORDER BY MOYENNE DESC
LIMIT 10
                  ''')

TOP10A2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP10 SERIE A3

# COMMAND ----------

TOP10A3=spark.sql('''
                  SELECT NOM, PRENOM, SEXE, MOYENNE, SERIE, ETABLISSEMENT
FROM BAC
WHERE SERIE ="A3"
ORDER BY MOYENNE DESC
LIMIT 10
                  ''')

TOP10A3.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## EFFECTIF PAR MAOYENNE

# COMMAND ----------

df2=spark.sql('''
              
              SELECT FLOOR(MOYENNE) AS MOYENNE_TRONQUEE, COUNT(*) AS EFFECTIF
              FROM BAC
              GROUP BY MOYENNE_TRONQUEE
              ''')
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOMBRE DES ADMIS PAR SERIE

# COMMAND ----------

df3=spark.sql('''
              
              SELECT SERIE, COUNT(*) AS NB_ADMIS_PAR_SERIE
              FROM BAC
              GROUP BY SERIE
              ORDER BY NB_ADMIS_PAR_SERIE DESC
              ''')
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP 20 : EFFECTIF PAR ETABLISSEMENT ET PAR ORDRE 

# COMMAND ----------

df4 = spark.sql('''
    SELECT 
        CASE 
            WHEN ETABLISSEMENT LIKE 'LYCEE THOMAS SANKARA%' THEN 'LYCEE THOMAS SANKARA'
           -- WHEN ETABLISSEMENT LIKE 'LYCEE DE MADINGO%' THEN 'LYCEE DE MADINGOU'
            ELSE ETABLISSEMENT
        END AS ETABLISSEMENT,
        COUNT(*) AS EFFECTIF
    FROM BAC
    GROUP BY 
        CASE 
            WHEN ETABLISSEMENT LIKE 'LYCEE THOMAS SANKARA%' THEN 'LYCEE THOMAS SANKARA'
                 -- WHEN ETABLISSEMENT LIKE 'LYCEE DE MADINGO%' THEN 'LYCEE DE MADINGOU'
            ELSE ETABLISSEMENT
        END
    ORDER BY EFFECTIF DESC
''')

df4.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NOM, PRENOM, DATE_DE_NCE
# MAGIC FROM BAC
# MAGIC LIMIT 10;
# MAGIC
