# Introduction sur les RDD
# import des librairies bumpy et py spark
import numpy as np
import time
from pyspark import SparkConf, SparkContext
# add est la fonction addition
from operator import add

# Initialisation de Spark Context 4 partitions
# Dans ce notebook, on utilisera SparkContext
# On verra que ce n'est pas la seule façon de faire.
conf = SparkConf().setAppName("TP3 exercice 1 RDD").setMaster("spark://spark-master:7077").set("spark.driver.memory", "1g").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")
#sc = SparkContext(master="local[4]")

# Initialisation d'un vecteur de 20 nbre aléatoires (entiers compris entre 0 et 9 inclus)
# utilisation d'une fonction numpy
A = np.random.randint(0,10,20)

# Creation d'un RDD parallelisé à partir du vecteur précédent  
# Le RDD est distribué sur les 4 partitions
RDD_A = sc.parallelize(A)

# Affichage fonction .collect()
# .collect renvoie une liste de tous les éléments du RDD (qui peut être afficher)
print("Liste des élements du RDD : ",RDD_A.collect())

# Comptons le nombre d'éléments
print("Nbre d'éléments de RDD_A : ",RDD_A.count())
# On retouve 20

# print(RDD_A) va juste indiquer le type du RDD
print("Type de RDD : ",RDD_A)

# Affichage des partitions (grâce à la fonction glom())
# .glom() va fusionner en considérant les élément de chaque partition
# .collect crée une liste 
# On remarque les 4 partitions
print("Liste des élements du RDD par partitions : ", RDD_A.glom().collect())
#time.sleep(200)

# on n'oublie d'arrêter le spark context
#sc.stop()


# On recommence la même chose avec 2 partitions
# On remarque qu'il n'y pplus 
# Initialisation de Spark Context 2 partitions
#sc = SparkContext(master="local[2]")

# Initialisation d'un vecteur de 20 nbre aléatoires (entiers)
A = np.random.randint(0,10,20)

# Creation d'un RDD parallelise a partir du vecteur précédent  
RDD_A = sc.parallelize(A)

# Affichage
print("Liste des élements du RDD : ",RDD_A.collect())

# print(RDD_A) va juste indiquer le type du RDD
print("Type de RDD : ",RDD_A)

# Affichage des partitions
# Ici, on remarque les deux partitions
print("Liste des élements du RDD par partitions : ",RDD_A.glom().collect())

# on n'oublie d'arrêter le spark context
#sc.stop()

# Initialisation de Spark Context 4 partitions
#sc = SparkContext(master="local[4]")

# Initialisation d'un vecteur de 20 nbre aléatoires (entierer)
A = np.random.randint(0,10,20)

# Creation d'un RDD parallelise a partir du vecteur précédent  
RDD_A = sc.parallelize(A)

# Affichage
print("Liste des élements du RDD : ",RDD_A.collect())

# Création d'un vecteur avec des valeurs uniques
RDD_A_Distinct = RDD_A.distinct()

print("")
# Affichage
# Les éléments de RDD_A_Distinct ne sont pas triés
print("Liste des élemnts (unique) de RDD_A : ",RDD_A_Distinct.collect())


print("")
# Somme des valeur de RDD_A
# Nous allons présenter 4 méthodes 
print("Somme des élements de RDD_A")
# Utilisation de la fonction sum()
print("Utilisation de sum() : ",RDD_A.sum())

# utilisation de la fonction reduce avec appliquant x + y
print("Utilisation de reduce(lambda x,y:x+y) : ",RDD_A.reduce(lambda x,y:x+y))

# utilisation de fold
# fold s'applique d'abord sur chaque partition puis effectue une réduction de l'ensemble des partitions réduites
# 0 est un valeur neutre
print("Utilisation de fold(0,lambda x,y:x+y) : ",RDD_A.fold(0,lambda x,y:x+y))

# utilisation de fold et de l'opérateur add
print("Utilisation de fold(0,add) : ",RDD_A.fold(0,add))

# On remarque que les résultats sont identiques

print("")

# filtrer en ne retenant que les nbres pairs
# Filter renvoie un nouveau RDD contenant uniquement les éléments qui satisfont à un prédicat.
# ici le prédicat est x%2==0 
print("Affichage des nbres pairs : ", RDD_A.filter(lambda x:x%2==0 ).collect())

print("")
# Affichge de quelques fonctions statistiques
# affichage du min, max, écart type, des stats
print(" min : ",RDD_A.min()," ; max : ", RDD_A.max()," ; stdev :", RDD_A.stdev())
print(" utilisation de stats() : ",RDD_A.stats())

print("")
# faire un map en mettant au carre
# a chaque élement du rdd, j'applique la fonction définie
print("Mise au carré du RDD ")
RDD_B = RDD_A.map(lambda x:x*x )

# afficher
print("Utilisation de map(lambda x:x*x ) : ",RDD_B.collect())

# définir une fonction
def square_x(x):
   return x*x

# faire un map en appliquant une fonction
RDD_B2 = RDD_A.map(square_x)

#Affichage
print("définition de square_x Utilisation de map(square_x ) : ", RDD_B2.collect())

print("")
# La fonction map es ttrès très souvent utilisée
# faire un map en mettant au carre
# a chaque élement du rdd, j'applique la fonction définie
RDD_BB = RDD_A.map(lambda x:(x,1) )

#Affichage
print("Ici, on crée (x,1) : ", RDD_BB.collect())


time.sleep(120)

# on n'oublie d'arrêter le spark context
sc.stop()




