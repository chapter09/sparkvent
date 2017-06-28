# spark-event-client
A library fetching and storing Apache Spark events in runtime 

```
            +-------------------------+
         +--|   Spark Events Server   |
         |  +-------------------------+
         |
         |  +-------------------------+
         +--|   Spark History Server  |
         |  +-------------------------+
         |
         |  +-------------+    +----------+
RESTful  +->|Sparkvent Lib|<---|  Config  |
  APIs      +-------------+    +----------+
                   |
                   v call      +----------+
            +-------------+    | redis DB |
            |    Script   |--> +----------+
            +-------------+    +----------+
                               |   file   |
                               +----------+
```
