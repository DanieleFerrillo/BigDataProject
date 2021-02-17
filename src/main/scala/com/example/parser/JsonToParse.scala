package com.example.parser


case class JsonToParse(
                        actor: Actor,
                        author: String,
                        repo: Repo,
                        `type`: String,
                        commit: Commit,
                        event: Event
                      )

/*
val schema = StructType(Seq(
      //StructField("event", StringType, false),
      StructField("actor", StringType, true),
      StructField("author", StringType, true),
      StructField("repo", (new StructType), true),
      StructField("type", StringType, true),
      StructField("commit", StringType, true)))

    val schema2 = (new StructType)
      //StructField("event", StringType, false),
      .add("actor", (new StructType))
      .add("author", StringType, true)
      .add("repo", StringType)
      .add("type", StringType)
      .add("commit", StringType, false)

    //val schema = ScalaReflection.schemaFor[JsonToParse].dataType.asInstanceOf[StructType]

    val jsonDF = sqlContext.read
      .schema(schema)
      .json(jsonStr)
*/
