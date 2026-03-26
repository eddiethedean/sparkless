# Sparkless - Fonctions manquantes pour la Data Platform

Gap analysis entre sparkless et `solya-data-platform-bronze-ingestion-multi-source`.
Les fonctions deja supportees (col, lit, when, coalesce, joins, window functions, create_map, map_keys, map_concat, broadcast, pivot, element_at, eqNullSafe, isin, between, etc.) ne sont pas listees ici.

---

## P0 - Critique (bloque les tests)

### 1. API Delta Merge chainee (style PySpark standard)

**Probleme** : La data platform utilise l'API standard Delta Lake avec des builders intermediaires :
```python
delta_table.merge(source, condition) \
    .whenMatched(condition).update(set_values) \
    .whenNotMatched(condition).insert(values) \
    .whenNotMatchedBySource(condition).delete() \
    .execute()
```

Sparkless n'a que l'API flat (`whenMatchedUpdate()`, `whenNotMatchedInsert()`) et `whenNotMatchedBySource` est completement absent.

**Fichier** : `sparkless/delta.py` (ligne 411, `DeltaMergeBuilder`)

**A implementer** :
- Classe `DeltaMergeMatchedActionBuilder` : `.update(set)`, `.updateAll()`, `.delete()`
- Classe `DeltaMergeNotMatchedActionBuilder` : `.insert(values)`, `.insertAll()`
- Classe `DeltaMergeNotMatchedBySourceActionBuilder` : `.update(set)`, `.delete()`
- Methodes `whenMatched(condition=None)`, `whenNotMatched(condition=None)`, `whenNotMatchedBySource(condition=None)` sur `DeltaMergeBuilder`
- Logique d'execution pour `whenNotMatchedBySource` dans `execute()`

---

### 2. Option `replaceWhere` pour le writer

**Probleme** : `df.write.option("replaceWhere", condition)` est utilise pour des ecrasements conditionnels. L'option est stockee mais jamais appliquee.

**Fichier** : `sparkless/dataframe/writer.py` (ligne 242, mode overwrite)

**A implementer** :
- Quand `save_mode == "overwrite"` et `replaceWhere` est present dans `_options` :
  1. Charger les donnees existantes
  2. Filtrer les lignes matchant la condition
  3. Concatener anciennes lignes (non-matchees) + nouvelles donnees
  4. Ecrire le resultat
- Utiliser `SafeExpressionEvaluator` pour parser la condition

---

### 3. `DataFrame.unpersist()`

**Probleme** : Appele dans la data platform, absent du DataFrame sparkless.

**Fichier** : `sparkless/dataframe/dataframe.py` (pres de `persist()`, ligne ~622)

**A implementer** : No-op retournant `self` (comme `cache()`/`persist()`).

---

### 4. `Catalog.dropTempView()` / `dropGlobalTempView()`

**Probleme** : Utilise dans 6+ fichiers de la data platform (conftest.py, enrichment utils, adapters...). Completement absent de sparkless.

**Utilisation data platform** :
- `tests/conftest.py:555`
- `pipelines/core/adapters/spark/local.py:330`
- `pipelines/layers/silver/shared/enrichment/shop_enrichment_utils.py:69`
- `pipelines/layers/silver/shared/enrichment/image_scraping_common.py:755`

**Fichier** : `sparkless/session/catalog.py`

**A implementer** :
- `dropTempView(viewName)` : supprimer la vue temporaire du storage, retourner `True` si elle existait
- `dropGlobalTempView(viewName)` : idem pour les vues globales

---

## P1 - Important (cause des echecs frequents)

### 5. `DataFrame.withMetadata(columnName, metadata)`

**Probleme** : Utilise dans `pipelines/core/utils/schema_helpers.py:145` pour la gestion de metadonnees de schema. Marque ❌ dans la matrice PySpark sparkless.

**Fichier** : `sparkless/dataframe/dataframe.py`

**A implementer** : Methode qui stocke les metadonnees sur le StructField de la colonne specifiee et retourne un nouveau DataFrame.

---

### 6. `DataFrameWriter.insertInto(tableName)`

**Probleme** : Utilise dans les tests de la data platform (`tests/core/adapters/spark/test_mock_dataframe.py:1050`). Seulement declare dans l'interface, pas implemente.

**Fichier** : `sparkless/dataframe/writer.py`
**Interface** : `sparkless/core/interfaces/dataframe.py:179`

**A implementer** : Inserer les donnees du DataFrame dans une table existante (equivalent a un `mode("append").saveAsTable(tableName)` mais en respectant l'ordre des colonnes de la table cible).

---

### 7. `DataFrameWriter.bucketBy(numBuckets, col, *cols)` et `.sortBy(col, *cols)`

**Probleme** : Utilise dans les tests de la data platform (`test_mock_dataframe.py:1037,1044`). Absent de sparkless.

**Fichier** : `sparkless/dataframe/writer.py`

**A implementer** :
- `bucketBy(numBuckets, col, *cols)` : stocker les infos de bucketing, retourner `self` (no-op pour le mock, l'important est de ne pas crasher)
- `sortBy(col, *cols)` : stocker les infos de tri, retourner `self` (no-op)

---

### 8. `df.stat.approxQuantile()` (DataFrameStatFunctions proxy)

**Probleme** : La data platform appelle `df.stat.approxQuantile()` (`pipelines/core/validation/registry.py:797`). Sparkless a `df.approxQuantile()` directement mais pas de proxy `.stat`.

**Fichier** : `sparkless/dataframe/dataframe.py`

**A implementer** :
- Classe `DataFrameStatFunctions` avec les methodes : `approxQuantile()`, `corr()`, `cov()`, `crosstab()`, `freqItems()`, `sampleBy()`
- Propriete `stat` sur DataFrame retournant une instance de `DataFrameStatFunctions` qui delegue au DataFrame

---

### 9. `StorageLevel` pour `persist()`

**Probleme** : La data platform definit un mock de `StorageLevel` dans `tests/conftest.py:117-131`. PySpark accepte `df.persist(StorageLevel.MEMORY_AND_DISK)`. Sparkless n'exporte pas `StorageLevel`.

**Fichier** : `sparkless/__init__.py` ou nouveau fichier `sparkless/storage_level.py`

**A implementer** :
- Classe `StorageLevel` avec les constantes : `DISK_ONLY`, `DISK_ONLY_2`, `DISK_ONLY_3`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_2`, `MEMORY_AND_DISK_DESER`, `MEMORY_ONLY`, `MEMORY_ONLY_2`, `MEMORY_ONLY_SER`, `MEMORY_ONLY_SER_2`, `OFF_HEAP`, `NONE`
- Accepter `StorageLevel` dans `persist(storageLevel=None)`

---

### 10. Joins multi-conditions avec operateur `&`

**Probleme** (TODO.md #4) : Tres frequent dans la data platform. Les conditions composees echouent.
```python
result.join(other, (result["col1"] == other["col1"]) & (result["col2"] == other["col2"]), "left")
```

**Fichier** : `sparkless/dataframe/joins/operations.py`

**A implementer** : Parser les `BooleanAnd` ColumnOperation dans le join condition handler.

---

### 11. References de colonnes par alias dans les joins

**Probleme** (TODO.md #1) : `F.col("alias.column")` dans les conditions de join.
```python
result.alias("r").join(stock.alias("s"), F.col("r.id") == F.col("s.id"), "left")
```

**Fichier** : `sparkless/dataframe/joins/operations.py`

**A implementer** : Resoudre les prefixes d'alias lors de la resolution des colonnes de join.

---

### 12. Cast string sur les aggregations

**Probleme** (TODO.md #3) : `F.sum("col").cast("long")` et `F.lit(None).cast("long")` echouent.

**Fichier** : `sparkless/functions/core/operations.py`

**A implementer** : Accepter les noms de types en string dans `.cast()` sur les resultats d'aggregation.

---

### 13. `applyInArrow()` sur DataFrame et GroupedData

**Probleme** : Utilise dans la data platform pour des UDFs vectorisees Arrow. Absent de sparkless.

**Fichiers** : `sparkless/dataframe/dataframe.py`, `sparkless/dataframe/grouped/base.py`

**A implementer** :
- `applyInArrow(func, schema)` : convertir en PyArrow table -> appeler func -> reconvertir
- Suivre le pattern de `applyInPandas()` dans `grouped/base.py`

---

### 14. Champs manquants sur les objets Catalog (`Table`, `Database`)

**Probleme** : PySpark retourne des namedtuples riches. Sparkless retourne des objets minimaux.

**Fichier** : `sparkless/session/catalog.py` (lignes 30-69)

**A implementer** :
- `Database` : ajouter `catalog`, `description`, `locationUri`
- `Table` : ajouter `catalog`, `description`, `tableType`, `isTemporary`

---

## P2 - Nice to have

### 15. Delta `optimize().executeCompaction()` / `.zOrderBy()`

**Probleme** : Certains patterns chainent ces methodes apres `optimize()`.

**Fichier** : `sparkless/delta.py` (ligne ~200)

**A implementer** : No-ops retournant `self` pour le chainage.

---

### 16. Conditions Column dans Delta `delete()` / `update()` / `merge()`

**Probleme** : Ces methodes n'acceptent que des strings, PySpark accepte aussi des objets `Column`.

**Fichier** : `sparkless/delta.py`

**A implementer** : Detecter les objets Column et evaluer l'expression.

---

### 17. Column-based drop (`df.drop(F.col("alias.col"))`)

**Probleme** (TODO.md #2) : `TypeError: 'Column' object is not iterable`.

**Fichier** : `sparkless/dataframe/dataframe.py`

**A implementer** : Extraire le nom de colonne depuis l'objet Column dans `drop()`.

---

### 18. Column object comme nom dans `withColumn`

**Probleme** (TODO.md #5) : `withColumn(ColumnObject, expr)` echoue.

**Fichier** : `sparkless/dataframe/dataframe.py`

**A implementer** : Extraire le nom string depuis l'objet Column.

---

### 19. Row constructor avec valeurs None

**Probleme** (TODO.md #6) : `createDataFrame([Row(field=None)])` ne peut pas inferer le type.

**Fichier** : `sparkless/session/session.py`

**A implementer** : Defaulter a `StringType()` quand toutes les valeurs d'une colonne sont `None` (comportement PySpark).

---

### 20. Options avancees des readers (CSV/JSON)

**Probleme** : Options `inferSchema`, `multiLine`, `encoding` non traitees.

**Fichier** : `sparkless/dataframe/reader.py`

---

## Deja supporte (pas de gap)

Les fonctions suivantes utilisees dans la data platform sont deja implementees dans sparkless :

**Fonctions F.\*** :
- `F.broadcast()`, `F.create_map()`, `F.map_concat()`, `F.map_keys()`, `F.map_values()`
- `F.element_at()`, `F.array_contains()`, `F.explode()`, `F.flatten()`, `F.size()`
- `F.col()`, `F.lit()`, `F.when()`, `F.otherwise()`, `F.coalesce()`
- `F.lower()`, `F.upper()`, `F.trim()`, `F.split()`, `F.concat()`, `F.concat_ws()`
- `F.regexp_replace()`, `F.substr()`, `F.substring()`
- `F.to_date()`, `F.to_timestamp()`, `F.date_format()`, `F.datediff()`, etc.
- `F.sum()`, `F.avg()`, `F.count()`, `F.min()`, `F.max()`, `F.countDistinct()`
- `F.row_number()`, `F.rank()`, `F.dense_rank()`, `F.lag()`, `F.lead()`
- `F.from_json()`, `F.to_json()`, `F.get_json_object()`
- `F.xxhash64()`, `F.hash()`
- `F.monotonically_increasing_id()`
- `F.try_divide()`, `F.try_to_timestamp()` (fonctions try_*)
- `F.struct()`, `F.named_struct()`

**DataFrame operations** :
- `Window.partitionBy()`, `.orderBy()`, `.rowsBetween()`, `.rangeBetween()`
- `df.groupBy().agg()`, `df.groupBy().pivot()`
- `df.join()` (inner, left, right, outer, cross, left_anti, left_semi)
- `df.union()`, `df.unionByName()`, `df.distinct()`, `df.dropDuplicates()`
- `df.cache()`, `df.persist()`, `df.repartition()`, `df.coalesce()`
- `df.toPandas()`, `df.collect()`, `df.toLocalIterator()`, `df.show()`, `df.printSchema()`
- `df.toDF()` (renommer colonnes), `df.alias()`, `df.transform()`
- `col.cast()` (avec DataType objects), `col.isin()`, `col.between()`
- `col.isNull()`, `col.isNotNull()`, `col.eqNullSafe()`
- `col.contains()`, `col.startswith()`, `col.endswith()`
- `col.getItem()`, `col.getField()`, `col.withField()`
- `col.desc_nulls_first()`, `col.asc_nulls_last()`, etc.
- `df.approxQuantile()` (direct, pas via `.stat`)

**Session & IO** :
- `spark.table()`, `spark.sql()`, `spark.createDataFrame()`
- `spark.read.parquet()`, `.csv()`, `.json()`
- `spark.catalog.tableExists()`, `.listTables()`, `.getTable()`, `.listDatabases()`
- `spark.catalog.setCurrentDatabase()`, `.currentDatabase()`
- `spark.catalog.cacheTable()`, `.uncacheTable()`, `.isCached()`, `.clearCache()`
- `spark.conf.set()`, `spark.conf.get()`
- `spark.range()`, `spark.version`, `spark.newSession()`
- `DeltaTable.forPath()`, `.forName()`, `.delete()`, `.update()`, `.optimize()`, `.vacuum()`
- `df.write.mode().format().saveAsTable()`, `.option("overwriteSchema", ...)`
- `df.write.partitionBy()`, `df.write.save()`
- `df.applyInPandas()`, `df.mapInPandas()`
- `@udf()`, `@pandas_udf()`

**Exceptions** :
- `AnalysisException`, `IllegalArgumentException`, `ParseException`

**Types** :
- Tous les types primitifs + `ArrayType`, `MapType`, `StructType`, `DecimalType(precision, scale)`

---

## Fonctionnalites NON utilisees par la data platform (pas besoin d'implementer)

- `readStream` / `writeStream` (Structured Streaming)
- `bitwiseAND` / `bitwiseOR` / `bitwiseXOR`
- `Column.dropFields()`
- `spark.udf.register()` (SQL UDF registration)
- `setCurrentCatalog` / `currentCatalog`
- `listColumns`, `listFunctions`, `functionExists`, `getFunction`
- `createExternalTable`
- `CalendarIntervalType`, `UserDefinedType`
- `F.typedLit()`, `F.array_join()`, `F.slice()`, `F.sequence()`, `F.make_date()`
- `F.transform()` (higher-order), `F.aggregate()`, `F.reduce()`
- `F.exists()`, `F.forall()`, `F.map_filter()`
- `F.schema_of_json()`, `F.schema_of_csv()`
- `F.input_file_name()`
- `F.typeof()`
- `df.foreach()`, `df.foreachPartition()`
- `df.observe()`, `df.hint()`, `df.colRegex()`
- `df.na.fill()`, `df.na.drop()`, `df.na.replace()` (la DP utilise `fillna()`/`dropna()` directement)
- `df.stat.*` autres que `approxQuantile`
- `SparkContext` direct
- `DataFrame.withWatermark()`

---

## Ordre d'implementation recommande

| Phase | Items | Priorite | Effort estime |
|-------|-------|----------|---------------|
| 1 | #3 unpersist, #4 dropTempView, #7 bucketBy/sortBy, #9 StorageLevel | P0/P1 (trivial) | ~1 jour |
| 2 | #1 Delta merge chainee, #2 replaceWhere | P0 (complexe) | ~2-3 jours |
| 3 | #5 withMetadata, #6 insertInto, #8 df.stat proxy, #10 joins & | P1 | ~2 jours |
| 4 | #11 alias refs, #12 string cast, #13 applyInArrow, #14 Catalog fields | P1 | ~2 jours |
| 5 | #15-20 reste P2 | P2 | ~1-2 jours |
