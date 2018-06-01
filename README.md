# Komodo
A Kotlin noSQL database with MVStore as the backend
## Why?
This project was started fill a need for a lightweight, efficient database in Kotlin that could be deployed anywhere a JVM is available.

The key objectives are:
* As few dependencies as possible
* No SQL
* Local file backend
* Reactive API
* Kotlin idioms
* B-tree and R-tree indices

## Backend
The underlying store is MVStore, from the H2 database project. This is a production quality, self-contained pure Java implementation of key-value
stores.
## POKO (Plain Old Kotlin Objects) interface
Each KoMap (equivalent to a table) in the database is intended to store one kind of object. The translation between the stored data (which will usually be in the form
of simple arrays of bytes) and Kotlin objects is done by a a CODEC (Coder/decoder) which is specific to each object. There is no default 
serialization or mapping - a CODEC must be written for each KoMap. The CODEC is also responsible for specifying any indices used. Consequently
Komodo does not depend on GSON, Jackson or any other serialization framework.
## Indices
The KoMap itself is a map of primary keys (Long data type) to objects. Indices are implemented by creating secondary maps of keys (as generated 
by the CODEC) to primary keys. There can be multiple indices for each KoMap and all insertions, deletions and updates will update all indices
automatically. Unique and non-unique indices are supported.

To be continued....
