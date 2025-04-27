# Mastering Snapshot Versioning in Apache Iceberg 🚀

Imagine having a time machine for your data tables — where you can instantly travel back to any version, experiment safely, and recover from mistakes effortlessly. That's the power of **Snapshot Versioning** in **Apache Iceberg**!

In this blog, we'll dive deep into **how versioning works in Iceberg**, **best practices** for managing snapshots, and **hands-on examples with code** to unleash its full potential.

---

## 📚 What is Snapshot Versioning in Iceberg?

Apache Iceberg is a revolutionary table format for huge analytic datasets. One of its superpowers is **Snapshot Management** — every write operation (insert, update, delete) creates a new **immutable snapshot** of the table.

Each snapshot captures:
- The **state** of the table at that time (which data files are included)
- **Metadata** like schema, partition spec, and sort order
- **Timestamps** and **operation summaries**

This enables you to:
- **Time-travel** queries
- **Roll back** to previous versions
- **Branch and tag** versions (like Git)
- **Audit** data evolution

---

## 🖊️ How Iceberg Organizes Snapshots

When you modify a table, Iceberg doesn't overwrite data files. Instead, it:
- Writes new **data files** (only for changed data)
- Updates **manifest files** (listing data files)
- Creates a new **snapshot** in the metadata.json

**Each snapshot has:**
- A **snapshot ID** (unique)
- A **parent snapshot ID** (for history tracking)
- A **timestamp**
- A **summary** (operation type: append, overwrite, delete, etc.)

---

## 📅 How to View and Manage Snapshots (with PySpark Code!)

Let's get hands-on! 🔧

Assuming you're using **PySpark** with Iceberg extensions enabled:

### 1. View All Snapshots

```python
# PySpark Example
spark.sql("DESCRIBE HISTORY mydb.mytable").show(truncate=False)
```

---

### 2. Time Travel to a Past Snapshot

Query data from an earlier point in time using:

```python
# Based on Snapshot Version
snapshot_id = 1234567890123
spark.read.format("iceberg").option("snapshot-id", snapshot_id).load("mydb.mytable").show()

# Or based on Timestamp
timestamp = "2024-04-20T12:00:00"
spark.read.format("iceberg").option("as-of-timestamp", timestamp).load("mydb.mytable").show()
```

---

### 3. Roll Back to a Previous Snapshot

You can **reset** your table's current state to an older snapshot:

```python
rollback_snapshot_id = 1234567890123
spark.sql(f"CALL system.rollback_to_snapshot('mydb.mytable', {rollback_snapshot_id})")
```

This doesn't delete data but updates the current metadata pointer to the selected snapshot. (Just like a `git reset`!)

---

### 4. Expire Old Snapshots (Cleanup)

Important: **Snapshots consume storage** (data + metadata files).

To clean up:

```python
# Expire snapshots older than a specific timestamp
expire_before = "2024-03-01 00:00:00"
spark.sql(f"CALL system.expire_snapshots('mydb.mytable', older_than => TIMESTAMP '{expire_before}')")
```

**Best practice:** Schedule automatic snapshot expiration for housekeeping.

---

## 🚷 Best Practices for Snapshot Versioning

| Best Practice | Why It Matters |
|:--------------|:---------------|
| **Use explicit timestamps for critical queries** | Avoid surprises from schema or data changes |
| **Limit number of retained snapshots** | Prevent storage bloat |
| **Automate expiration** | Keep metadata clean and lightweight |
| **Use branching/tags for experiments** | Safely isolate dev/test pipelines |
| **Monitor snapshot size and growth** | Detect anomalies early |

---

## 🛰️ Real-world Example: Protecting Critical Reports

A FinTech company built monthly financial reports on an Iceberg table.
- Each month, they "tagged" a snapshot after closing books.
- Analysts queried the **tag** for a stable version.
- Even if source data changed later, their reports stayed accurate!

**Iceberg Command to create a Tag (using PySpark):**

```python
snapshot_id_for_tag = 1234567890123
spark.sql(f"CALL create_tag('mydb.mytable', 'month_end_closure', snapshot_id => {snapshot_id_for_tag})")
```

---

## 🔗 References and Further Reading

- [Apache Iceberg Official Documentation](https://iceberg.apache.org/docs/latest/)
- [Managing Snapshots - Iceberg Docs](https://iceberg.apache.org/docs/latest/snapshots/)
- [PyIceberg GitHub Repository](https://github.com/apache/iceberg-python)
- [Branching and Tagging in Iceberg](https://iceberg.apache.org/docs/latest/branch-tag/)

---

## 🎉 Conclusion: Empower Your Data with Versioning

Snapshot Versioning in Apache Iceberg isn't just a nice-to-have — it's a **must-have** for modern data platforms.

It provides:
- **Flexibility** (time travel, rollback)
- **Safety** (experimentation without data loss)
- **Efficiency** (minimal write amplification)

Master it — and you truly master the art of **data agility**!

---

#ReadyToTimeTravel? 🌐
