# pagerank
Compute pagerank on a subset of wikipedia pages.


## Reproduction de l'expérience

Configurer d'abord la machine virtuelle. Pour se faire :

1. <a href="#création-du-bucket">Créer un bucket à partir de l'interface utilisateur.</a>
2. Exporter les variables d'environnement nécessaire.
3. Se placer dans le répertoire du projet `pagerank`.


#### Création du bucket

Se rendre sur la page [Buckets - Cloud Storage](https://console.cloud.google.com/storage/browser?referrer=search&cloudshell=true&hl=fr&prefix=&forceOnBucketsSortingFiltering=true&bucketType=live). Créer un nouveau bucke 
```bash
export PROJECT_ID=your-project-id
export BUCKET=your-bucket-id
```

##
#### Pour exécuter un cluster de 6 noeuds


Ajouter le rôle "Nœud de calcul Dataproc"

storage.objects.get
storage.objects.update
