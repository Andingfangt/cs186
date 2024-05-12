# DIS 13

## REVIEW

### 1. OLTP and OLAP and NoSQL

**Online Transaction Processing (OLTP)**:

* This kind of workload is common to “frontend” applications such as social networks and online stores.
* Queries are typically simple lookups, inserts and updates, rarely include joins.

**Online Analytical Processing (OLAP)**:

* *read-only*
* typically involves large numbers of joins and aggregations in order to support decision making.

**Not Only SQL(NoSQL)**:

* scale better than traditional relational databases
* can handle a higher volume of simple updates and queries.
* not allowed joins.

### 2. Methods for Scaling Databases

**Partitioning**: database be partitioned into segments of data that are spread out over multiple machines.

* good for write-heavy workloads
* bad for read-heavy workloads since join on relations partitioned across many machines requiring costly network transfer

**Replication**: the data is replicated on multiple machines.

* effective for read-heavy workloads.
* bad for write-heavy workloads, since we need write to each replica.
* more resilient to data loss.

### 3. BASE for NoSQL

different from **ACID** in Relational DB

* **B**asic **A**vailability: Application must handle partial failures itself
* **S**oft State: DB state can change even without inputs
* **E**ventually Consistency: DB will "eventually" become consistent

### 4. NoSQL Data Models

**Key-Value Stores(KVS)**:

* Key: typically a string or integer that uniquely identifies the record.
* Value: a variety of field types that can be convert to byte-array values.
* Provides just two operations: $get(key)$ and $put(key, value)$.

**Wide-Column Stores (or Extensible Record Stores)**:

* Unlike a relational database, wide-column stores do not require each row in a table to have the same columns.
* Can think as a 2-dimensional key-value store:
  * key = rowID, value = record
  * key = (rowID, columnID), value = field
* Provided operations are the same as a KVS: $get(key)$ (or $get(key, [columns]$) and $put(key, value)$.

**Document Stores**:

* KVS whose values adhere to a semi-structured data format such as JSON, XML or Protocol Buffers are termed document stores.
* The values are called documents, different from Relational DB store tuples in tables.




## 1. OLTP vs OLAP

(a) OLTP, when a user likes or dislikes a post, the site would need to do a lookup on that post and update its likes/dislikes value.

(b) OLAP, since it need do aggregations and analyze.

(c) OLAP, since we need do analyze and read-only the related data.

## 2. Scaling

(a) use Partitioning since it these workloads involve lot of write but few reads.

(b) use Replication since it has more resilient to data loss.

## 3. BASE

(a) None, This is in fact the Soft State and Eventual Consistency properties in action. As the write propagates through the system, reads may return different values because the database is in-consistent.

(b) Basic Availability, since valid inputs sometimes receive an error response.

(c) Eventual Consistency, should write to all replicas.

(d) None, since this is a empty database.

## 4. Key-Value Stores

(a)

1. $hash(Key=sid1)$ to get which partition the data is stored on.
2. get the value from any of the replicas/servers.

(b)

1. $hash(Key=sid2)$ to get which partition the data should put.
2. put the value to all of the replicas/servers.
3. Note: the propagation of changes may not happen immediately, we only need to enforce Eventual Consistency.

(c)

No. Since we’re only enforcing eventual consistency, the changes from the put operation may
not have propagated to all replicas yet.


## 5. JSON

(a)

```JSON
{
    "Players": [
    {
     "name": "Tony", 
     "debut": 
     "10/12/09", 
     "goals": 43
    },
    {
     "name": "Katy", 
     "debut": "1/20/14", 
     "goals": 22
    },
 ]
}
```

(b)

Table1: Players(name, debut)

| name | debut |
| :-: | :-: |
| Abby | 10/12/09 |
| Babby | 1/20/14 |
| Cabby | 1/21/14 |

Table2: Goals(name, goals)

| name | goals |
| :-: | :-: |
| Abby | 43 |
| Babby | 22 |
| Cabby | 23 |

## 6. Mongo Query Language (MQL)

```JavaScript
db.teams.aggregations ({
    {$match: {
        divisionsID: 1, // find divisionID == 1;
        wins: {$gte: 10} // find wins >= 10;
    }},
    {$sort: {
        "coach": -1, // sorted by coach DESC
        "caption": 1 // broken by captain ASC.
    }},
    {$project: {
        "coach": 1,
        "caption": 1, // Include the "coach" and "captain" fields in the output documents.
        "_id": 0 // Exclude the "_id" field from the output documents (MongoDB includes the "_id" field by default)
    }}
})
```

```JavaScript
db.teams.aggregations ({
    // Find stadiumCapacity >= 2000
    {$match: {
        stadiumCapacity: {$gte: 2000} 
    }},
    // Groups the documents by the "divisionId" field. 
    // Within each group, it calculates the maximum value of the "wins" field and counts the number of documents.
    {$group: {
        _id: "$divisionsID",
        maxWins: {$max: "wins"},
        count: {$sum: 1}
    }},
    // sorted by maxWins ASC and broken ties by count DESC.
    {$sort: {
        maxWins: 1,
        count: -1
    }},
    // Projects the fields in the output documents. 
    // It includes only the "div" field (renamed from "_id") and the "maxWins" field. 
    // Excluded the "_id" field from the output.
    {$project: {
        div: "$_id",
        maxWins: 1,
        _id: 0
    }}
})
```

