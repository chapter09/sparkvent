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
         |  +-------------+  +--------+
RESTful  |  |             |  |        |
  APIs   +->|Sparkvent Lib|<-| Config |
            |             |  |        |
            +------+------+  +--------+
                   |
                   v           +----------+
            +-------------+    | redis DB |
            |    Script   |--> +----------+
            +-------------+    +----------+
                               |   file   |
                               +----------+
```