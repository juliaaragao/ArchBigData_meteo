#start the SparkContext
import sys
from operator import add

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TP3 exercice 2 text').getOrCreate()


# lecture des fichiers
# n'oublier pas les copier dans hfs
text_file1 = spark.sparkContext.textFile("/opt/spark-apps/holmes.txt")
text_file2 = spark.sparkContext.textFile("/opt/spark-apps/frankenstein.txt")


#for element in text_file1.collect():
#    print(element)

# flatMap le fichier dans words
words1 = text_file1.flatMap(lambda line: line.split(" "))
words2 = text_file2.flatMap(lambda line: line.split(" "))


# filtrer les mots vides ''
not_empty1 = words1.filter(lambda x: x!='')
not_empty2 = words2.filter(lambda x: x!='')

# prparer pour chaque mot du texte le couple (mot,1)
key_values1 = not_empty1.map(lambda word: (word, 1))
key_values2 = not_empty2.map(lambda word: (word, 1))
#key_values=  à compléter.map(lambda word: (word, 1))

# compter le nombre de mots du texte
# à compléter
wt1 = not_empty1.count()
wt2 = not_empty2.count()

print(not_empty1.count())

print(not_empty2.count())
# réduire les couples (word,1)  en comptant  permettant de compter les mots
counts1 = key_values1.reduceByKey(add)
counts2 = key_values2.reduceByKey(lambda a, b: a+b)

print(counts1.sortBy(lambda x: x[1],ascending=False).take(10))
print(counts2.sortBy(lambda x: x[1],ascending=False).take(10))
# calcul la probabilité d'appariation de chaque mot
# à compléter
key_pb1 = counts1.map(lambda x: (x[0], x[1]/wt1))

print(key_pb1.sortBy(lambda x: x[1],ascending=False).take(10))


# refaire la même pour le deuxième texte

# pour chaque texte afficher
# le nombre de mots
# le nombre de mots distincts
                                                                   
print(counts1.sortBy(lambda x: x[1],ascending=False).take(10))
print(counts2.sortBy(lambda x: x[1],ascending=False).take(10))
# calcul la probabilité d'appariation de chaque mot
# à compléter
key_pb1 = counts1.map(lambda x: (x[0], x[1]/wt1))

print(key_pb1.sortBy(lambda x: x[1],ascending=False).take(10))


# refaire la même pour le deuxième texte

# pour chaque texte afficher
# le nombre de mots
# le nombre de mots distincts

print(counts1.count())
print(counts2.count())
# l'occurence des 10-15 mots les fréquents
# la probabilité des 10-15 mots lesplus fréquents

spark.stop()




